from kafka import KafkaProducer
import sys

host = sys.argv[1] if len(sys.argv[1:2]) else "localhost"
port = sys.argv[2] if len(sys.argv[2:3]) else "9094"

host_and_port = f"{host}:{port}"
topic = 'foobar'

producer = KafkaProducer(
    bootstrap_servers=host_and_port
)

for i in range(10):
    try:
        message = b"some_message_bytes"

        future = producer.send(topic, message)
        result = future.get(timeout=60)
        print(result, message)

    except Exception as e:
        print(e)

producer.flush()