from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
from report_pb2 import *
from datetime import datetime

import time
import weather

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1
try:
    admin_client.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])
    print("Created topics successfully")
except TopicAlreadyExistsError:
    print("already exists")

time.sleep(3)

print("Topics:", admin_client.list_topics())

producer = KafkaProducer(
    bootstrap_servers=[broker],
    retries=10,
    acks='all'
)

for date, degrees in weather.get_next_weather(delay_sec=0.1):
    value = Report(date=date, degrees=degrees).SerializeToString()
    producer.send("temperatures", value, bytes(datetime.strptime(date, '%Y-%m-%d').strftime("%B"), "utf-8"))
    # time.sleep(1)