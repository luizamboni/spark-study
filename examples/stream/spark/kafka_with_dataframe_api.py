
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
import argparse
from typing import Optional
class ArgsInterface:
  host: Optional[str]
  topic: Optional[str]

  def __init__(self, host: str, topic: str):
      self.host = host
      self.topic = topic

def get_args() -> ArgsInterface:

  parser = argparse.ArgumentParser()
  parser.add_argument('--host')
  parser.add_argument('--topic')

  args = parser.parse_args()

  return ArgsInterface(args.host, args.topic)

args = get_args()

spark = SparkSession.builder.appName("streamReader").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ds = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", args.host) \
  .option("subscribe", args.topic) \
  .option("startingOffsets", "earliest") \
  .load()

formated_df = ds.selectExpr(
  "CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp", "timestampType"
)

## complete output mode
grouped_df = formated_df.groupBy('key') \
                        .count() \

grouped_df.writeStream.format("console") \
                      .outputMode("complete") \
                      .start()

## append with whatermark
grouped_df = formated_df.withWatermark("timestamp", "10 seconds") \
                        .groupBy(
                          window(col('timestamp'), "10 seconds", "5 seconds"),
                          col('key')
                        ) \
                        .count() \
                        .writeStream \
                        .format("console") \
                        .outputMode("append") \
                        .start()

writeStream = formated_df.writeStream \
                        .format("console")

# comment and uncomment to use other trigger strategies
# writeStream = writeStream.trigger(processingTime='20 seconds')
# writeStream = writeStream.trigger(once=True)
# writeStream = writeStream.trigger(continuous='10 second')

writeStream = writeStream.option(
  "checkpointLocation", 
  "file:////home/project/examples/stream/spark/_checkpoints/a",
)

writeStream.start() \
           .awaitTermination()
  
