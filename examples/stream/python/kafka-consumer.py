from kafka import KafkaConsumer
import argparse
from typing import Optional
class ArgsInterface:
  topic: Optional[str]
  host: Optional[str]
  group_id: str
  offset: str
  length: Optional[int]
  
  def __init__(self, topic: Optional[str], host: Optional[str], group_id: str, offset: str, length: Optional[int]):
    self.topic = topic
    self.host = host
    self.group_id = group_id
    self.offset = offset
    self.length = length

def get_args() -> ArgsInterface:

  parser = argparse.ArgumentParser()
  parser.add_argument('--topic', default=None)
  parser.add_argument('--host', default=None)
  parser.add_argument('--group_id', default="teste")
  parser.add_argument('--offset', default="lastest")
  parser.add_argument('--length', default=None, type=int)

  

  args = parser.parse_args()

  return ArgsInterface(
    args.topic, 
    args.host, 
    args.group_id, 
    args.offset, 
    args.length
  )

args = get_args()

partition = 0

consumer = KafkaConsumer(
    args.topic,
    bootstrap_servers=args.host,
    auto_offset_reset=args.offset,
    group_id=args.group_id
)

count = 0
for msg in consumer:
    if args.length and count > args.length:
      break
    else:
      count += 1
  
    print(msg.offset)
    # print(msg)


