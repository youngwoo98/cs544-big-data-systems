import grpc
import station_pb2_grpc, station_pb2
from concurrent import futures
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import ConsistencyLevel, Unavailable

class StationRecord:
    def __init__(self, tmin, tmax):
        self.tmin = tmin
        self.tmax = tmax

class StationServer(station_pb2_grpc.StationServicer):
    def __init__(self):
        cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.cass = cluster.connect('weather')
        self.insert_statement = self.cass.prepare("""
        INSERT INTO stations (id, date, record)
        VALUES (?, ?, ?)
        """)
        self.insert_statement.consistency_level = ConsistencyLevel.ONE
        self.max_statement = self.cass.prepare("""
        SELECT MAX(record.tmax) FROM stations WHERE id = ?
        """)
        self.max_statement.consistency_level = ConsistencyLevel.ALL
        self.cass.cluster.register_user_type('weather', 'station_record', StationRecord)
    
    def RecordTemps(self, request, context):
        try:
            record = StationRecord(tmin=request.tmin, tmax=request.tmax)
            self.cass.execute(self.insert_statement, (request.station, request.date, record))
            return station_pb2.RecordTempsReply(error = "")
        except Unavailable as ue:
            return station_pb2.RecordTempsReply(error= f'need {ue.required_replicas} replicas, but only have {ue.alive_replicas}')
        except NoHostAvailable as ne:
            for evalue in ne.errors.values():
                return station_pb2.RecordTempsReply(error= f'need {evalue.required_replicas} replicas, but only have {evalue.alive_replicas}')
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))
            
    def StationMax(self, request, context):
        try:
            result = self.cass.execute(self.max_statement, (request.station, ))
            row = result.one()
            if row:
                return station_pb2.StationMaxReply(tmax = row[0], error="")
            else:
                return station_pb2.StationMaxReply(tmax = 0, error= "No data")
        except Unavailable as ue:
            return station_pb2.StationMaxReply(tmax = 0, error= f'need {ue.required_replicas} replicas, but only have {ue.alive_replicas}')
        except NoHostAvailable as ne:
            for evalue in ne.errors.values():
                return station_pb2.StationMaxReply(tmax = 0, error= f'need {evalue.required_replicas} replicas, but only have {evalue.alive_replicas}')
        except Exception as e:
            return station_pb2.StationMaxReply(tmax = 0, error=str(e))
            
if __name__ == '__main__':
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
    station_pb2_grpc.add_StationServicer_to_server(StationServer(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    print("started")
    server.wait_for_termination()