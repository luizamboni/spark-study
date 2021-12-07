from kafka import KafkaConsumer
import argparse
from typing import Optional
class ArgsInterface:
  topic: Optional[str]
  host: Optional[str]
  
  def __init__(self, topic: Optional[str], host: Optional[str]):
    self.topic = topic
    self.host = host

def get_args() -> ArgsInterface:

  parser = argparse.ArgumentParser()
  parser.add_argument('--topic', default=None)
  parser.add_argument('--host', default=None)
  args = parser.parse_args()

  return ArgsInterface(args.topic, args.host)

args = get_args()

partition = 0

consumer = KafkaConsumer(
    args.topic,
    bootstrap_servers=args.host,
    auto_offset_reset='earliest',
    group_id="teste"
    # auto_offset_reset=-10
)

for msg in consumer:
    print(msg)


