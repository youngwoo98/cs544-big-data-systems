from kafka import KafkaConsumer
from report_pb2 import Report

broker = 'localhost:9092'

consumer = KafkaConsumer(bootstrap_servers=[broker])
consumer.subscribe(topics=['temperatures'])

for message in consumer:
    line = {}
    line['partition'] = message.partition
    line['key'] = message.key.decode('utf-8')
    val = Report.FromString(message.value)
    line['date'] = val.date
    line['degrees'] = val.degrees
    print(line)