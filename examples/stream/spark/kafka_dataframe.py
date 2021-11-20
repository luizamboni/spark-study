import sys
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 4:
      print(
        "Usage: network_wordcount.py <hostname> <port>", 
        file=sys.stderr
      )
      sys.exit(-1)
    
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    sc.setLogLevel("ERROR")
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    topic = sys.argv[3]

    per_topic_partitions = {
        topic: 1,
    }

    ssc = StreamingContext(sc, 10)
    kafkaStream = KafkaUtils.createStream(
      ssc, 
      f"{host}:{port}", 
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