from kafka import KafkaProducer
import argparse
from typing import Optional
class ArgsInterface:
  host: Optional[str]
  topic: Optional[str]
  
  def __init__(self, host: str, topic: str):
    self.topic = topic
    self.host = host

def get_args() -> ArgsInterface:

  parser = argparse.ArgumentParser()
  parser.add_argument('--host')
  parser.add_argument('--topic')
  args = parser.parse_args()

  return ArgsInterface(args.host, args.topic)

args = get_args()
# print(args.host)
# print(args.topic)

producer = KafkaProducer(
    bootstrap_servers=args.host
)

for i in range(10):
    try:
        message = b"some_message_bytes"
        key = b"key-value"
        print("before send")
        future = producer.send(
            args.topic,
            key=key,
            value=message
        )

        result = future.get(timeout=60)
        print(result)
    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")

producer.flush()