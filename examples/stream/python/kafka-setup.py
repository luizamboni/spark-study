from kafka.admin import KafkaAdminClient, NewTopic
import time
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

admin_client = KafkaAdminClient(
    bootstrap_servers=args.host, 
    client_id='test'
)

time.sleep(1)
try:
    admin_client.delete_topics([args.topic])
except:
    pass

time.sleep(1)

try: 
    admin_client.create_topics(
        new_topics=[
            NewTopic(
                name=topic, 
                num_partitions=1, 
                replication_factor=1,
                topic_configs= {
                    "cleanup.policy": "compact",
                    # "delete.retention.ms": "1000", 
                    # "segment.ms": "100",
                    # "min.cleanable.dirty.ratio": "0.01",
                }
            )
        ],
        validate_only=False
    )
    print("topic created")
except Exception as err:
    print(err)