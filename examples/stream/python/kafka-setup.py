from kafka.admin import KafkaAdminClient, NewTopic
import time, sys

host = sys.argv[1] if len(sys.argv[1:2]) else "localhost"
port = sys.argv[2] if len(sys.argv[2:3]) else "9094"
topic = sys.argv[3] if len(sys.argv[3:4]) else "foobar"


host_and_port = f"{host}:{port}"

admin_client = KafkaAdminClient(
    bootstrap_servers=host_and_port, 
    client_id='test'
)

time.sleep(1)
try:
    admin_client.delete_topics([topic])
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