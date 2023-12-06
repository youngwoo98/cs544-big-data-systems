from kafka import KafkaConsumer, TopicPartition
from report_pb2 import Report
from os.path import exists
from datetime import datetime

import os
import sys
import json

partitions = []
# except /files/consumer.py
for arg in sys.argv[1:]:
    partitions.append(int(arg))

broker = 'localhost:9092'

data = {}
# check for .json files.
# If it doesn't exist, make one with partition set to N and offset set to 0
for p in partitions:
    file = f'/files/partition-{p}.json'
    if not exists(file):
        with open(file, 'w') as fp:
            json.dump({
                "partition": int(p),
                "offset": 0
            }, fp)
    with open(file, 'r') as fp:
        data[p] = json.load(fp)
        
# counts = {}   # key=month, value=count
    
consumer = KafkaConsumer(bootstrap_servers=[broker])
consumer.assign([TopicPartition("temperatures", p) for p in partitions])

for p in partitions:
    tp = TopicPartition("temperatures", data[p]['partition'])
    consumer.seek(tp, data[p]['offset'])

while True:      # TODO: loop forever
    # for p in partitions:
    #     consumer.seek(TopicPartition("temperatures", data[p]['partition']), data[p]['offset'])
    batch = consumer.poll(1000)
    for tp, messages in batch.items():
        for msg in messages:
            # print(msg.offset)
            # print(tp)
            r = Report.FromString(msg.value)
            # print(r)
            # print(msg.partition)
            # print(data[msg.partition])
            year = datetime.strptime(r.date,'%Y-%m-%d').strftime("%Y")
            # print((year))
            if not msg.key.decode('utf-8') in data[msg.partition]:
                data[msg.partition][msg.key.decode('utf-8')] = {year: {
                    "count": 0,
                    "sum": 0,
                    "avg": 0,
                    "end": r.date,
                    "start": r.date
                }}
            elif not year in data[msg.partition][msg.key.decode('utf-8')]:
                data[msg.partition][msg.key.decode('utf-8')][year] = {
                    "count": 0,
                    "sum": 0,
                    "avg": 0,
                    "end": r.date,
                    "start": r.date
                }
            if data[msg.partition][msg.key.decode('utf-8')][year]['end'] < r.date or data[msg.partition][msg.key.decode('utf-8')][year]['count'] == 0:
                data[msg.partition][msg.key.decode('utf-8')][year]['count'] += 1
                data[msg.partition][msg.key.decode('utf-8')][year]['sum'] += r.degrees
                data[msg.partition][msg.key.decode('utf-8')][year]['avg'] = data[msg.partition][msg.key.decode('utf-8')][year]['sum']/data[msg.partition][msg.key.decode('utf-8')][year]['count']
                data[msg.partition][msg.key.decode('utf-8')][year]['end'] = r.date
            
            
            if msg == messages[-1]:
                data[msg.partition]['offset'] = msg.offset
            p = msg.partition
            path = f'/files/partition-{p}.json'
            path2 = path + ".tmp"
            with open(path2, "w") as f:
                json.dump(data[p], f)
                os.rename(path2, path)
    # print(data[0])
    # print(counts)