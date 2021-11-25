from kafka import KafkaProducer
import sys

host = sys.argv[1] if len(sys.argv[1:2]) else "localhost"
port = sys.argv[2] if len(sys.argv[2:3]) else "9094"
topic = sys.argv[3] if len(sys.argv[3:4]) else "foobar"

host_and_port = f"{host}:{port}"

producer = KafkaProducer(
    bootstrap_servers=host_and_port
)

for i in range(10):
    try:
        message = b"some_message_bytes"
        key = b"key-value"
        print("before send")
        future = producer.send(
            topic,
            key=key,
            value=message
        )

        result = future.get(timeout=60)
        print(result)
    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")

producer.flush()