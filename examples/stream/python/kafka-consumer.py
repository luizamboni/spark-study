from sys import argv
from kafka import KafkaConsumer
import sys

host = sys.argv[1] if len(sys.argv[1:2]) else "localhost"
port = sys.argv[2] if len(sys.argv[2:3]) else "9094"
topic = sys.argv[3] if len(sys.argv[3:4]) else "foobar"

host_and_port = f"{host}:{port}"
partition = 0

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=host_and_port,
    auto_offset_reset='earliest',
    group_id="teste"
    # auto_offset_reset=-10
)

for msg in consumer:
    print(msg)


