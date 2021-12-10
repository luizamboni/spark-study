from kafka import KafkaProducer
import argparse
from typing import Optional
class ArgsInterface:
  host: str
  topic: str
  volume: int

  def __init__(self, host: str, topic: str, volume: int, infinity: bool):
    self.topic = topic
    self.host = host
    self.volume = volume
    self.infinity = infinity

def get_args() -> ArgsInterface:

  parser = argparse.ArgumentParser()
  parser.add_argument('--host')
  parser.add_argument('--topic')
  parser.add_argument('--volume', default = 10, type = int)
  parser.add_argument('--infinity', type = bool, default = False)

  args = parser.parse_args()

  return ArgsInterface(args.host, args.topic, args.volume, args.infinity)

args = get_args()

producer = KafkaProducer(
    bootstrap_servers=args.host
)

def produce(topic: str, volume: int, count: int):
    print(count, "-" * 50)
    for i in range(volume):
        try:
            message = b"some_message_bytes"
            key = b"key-value"
            future = producer.send(
                topic,
                key=key,
                value=message
            )

            # result = future.get(timeout=60)
            # print(result)
        except Exception as err:
            print(f"Unexpected {err}, {type(err)}")

        producer.flush()


count = 1

produce(args.topic, args.volume, count)

while args.infinity:
    count += 1
    produce(args.topic, args.volume, count)


    