import torch
import threading
from collections import OrderedDict

import grpc
import modelserver_pb2_grpc, modelserver_pb2
import numpy as np
from concurrent import futures

class PredictionCache():

    coefs = None
    lock = threading.Lock()
    cache = OrderedDict()

    def SetCoefs(self, coefs):
        self.coefs = coefs 
        self.cache.clear()

    def Predict(self, X):
        round_X =torch.round(X, decimals = 4)
        X_tuple = tuple(round_X.flatten().tolist())

        with self.lock:
        
            #hit
            if X_tuple in self.cache:
                return self.cache[X_tuple], True
            
            #miss
            print(round_X, self.coefs)
            y = torch.mm(round_X, self.coefs)
            self.cache[X_tuple] = y

            #evict
            if(len(self.cache) > 10):
                self.cache.popitem(last=False)
            
            return y, False

class ModelServer(modelserver_pb2_grpc.ModelServerServicer):

    cache = PredictionCache()

    def SetCoefs(self, request, context):
        try:
            coefs = torch.tensor(request.coefs, dtype=torch.float32).reshape(-1, 1)
            self.cache.SetCoefs(coefs = coefs)
            return modelserver_pb2.SetCoefsResp(error = "")
        except Exception as e:
            return modelserver_pb2.SetCoefsResp(error= str(e))

    def Predict(self, request, context):
        try:
            X_tensor = torch.tensor(X = equest.X).reshape(1, -1)
            y, hit = self.cache.Predict(X_tensor)
            return modelserver_pb2.PredictResp(y= y.item(), hit= hit, error= "")
        except Exception as e:
            return modelserver_pb2.PredictResp(y= 0, hit= False, error= str(e))

server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
modelserver_pb2_grpc.add_ModelServerServicer_to_server(ModelServer(), server)
server.add_insecure_port("[::]:5440", )
server.start()
print("started")
server.wait_for_termination()
