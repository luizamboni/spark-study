from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
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


if __name__ == "__main__":

    args = get_args()
    topic = args.topic
    host = args.host

    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    sc.setLogLevel("ERROR")
    
    per_topic_partitions = {
        topic: 1,
    }

    ssc = StreamingContext(sc, 10)
    kafkaStream = KafkaUtils.createStream(
      ssc, 
      host, 
      "spark-streaming",  # consumer group_id
      per_topic_partitions
    )

    print("stream created")
    # connect with a tcp stream in server

    kafkaStream.pprint()
    counts = kafkaStream \
                  .map(lambda word: (word[1], 1))\
                  .reduceByKey(lambda a, b: a+b)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()