import modelserver_pb2_grpc, modelserver_pb2
import grpc
import sys
import threading

#python3 client.py 5440 "1.0,2.0,3.0" x1.csv x2.csv x3.csv
# print(sys.argv[0]) client.py
# print(sys.argv[1]) 5440
# print(list(map(float, sys.argv[2].split(",")))) 
# print(sys.argv[3:]) ['x1.csv', 'x2.csv', 'x3.csv']

channel = grpc.insecure_channel("localhost:"+ sys.argv[1])
stub = modelserver_pb2_grpc.ModelServerStub(channel)

req_coefs = list(map(float, sys.argv[2].split(",")))
stub.SetCoefs(modelserver_pb2.SetCoefsRequest(coefs = req_coefs))

c_hit = 0
total = 0

def process_thread(csv_file):

    global c_hit, total

    f = open(csv_file,'r')
    lines = f.readlines()
    for line in lines:
        print(line)
        X = list(map(float,line.split(",")))
        resp = stub.Predict(modelserver_pb2.PredictRequest(X=X))
        print(resp)
        if resp.hit:
            c_hit += 1            
        total += 1
    f.close()

csv_files = sys.argv[3:]
threads = []

for csv_file in csv_files:
    #@@@@@@@@@@@@@@args=(csv_file,) 왜?
    t = threading.Thread(target=process_thread, args=(csv_file,))
    t.start()
    threads.append(t)

for thread in threads:
    thread.join()
print(c_hit,total)
if total != 0:
    output = c_hit/total
else:
    output = 0
print(output)
