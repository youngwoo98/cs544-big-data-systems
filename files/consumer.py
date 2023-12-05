from kafka import KafkaConsumer, TopicPartition
from report_pb2 import Report
from threading import Thread, Lock

import sys


partitions = []
# except /files/consumer.py
for arg in sys.argv[1:]:
    partitions.append(int(arg))

broker = 'localhost:9092'

# consumer = KafkaConsumer(bootstrap_servers=[broker])
# consumer.subscribe(topics=['temperatures'])
# consumer.assign(partitions)

# for message in consumer:
    # print(message.partition)


lock = Lock()
def Print(*args):
    with lock:
        print(*args)

def temperature_consumer(partitions=[]):
    counts = {}   # key=month, value=count
    
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    consumer.assign([TopicPartition("temperatures", p) for p in partitions])
    while True:      # TODO: loop forever
        batch = consumer.poll(1000)
        for tp, messages in batch.items():
            for msg in messages:
                r = Report.FromString(msg.value)

                if not msg.key.decode('utf-8') in counts:
                    counts[msg.key.decode('utf-8')] = 0
                counts[msg.key.decode('utf-8')] += 1
        Print(partitions, counts, p)
# threading.Thread(target=animal_consumer, args=([0,1],)).start()
# threading.Thread(target=animal_consumer, args=([2,3],)).start()

def main():
    temperature_consumer(partitions)

if __name__ == "__main__":
    main()