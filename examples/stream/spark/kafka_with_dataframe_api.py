
from pyspark.sql import SparkSession
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

spark = SparkSession.builder.appName("streamReader").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ds = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", args.host) \
  .option("subscribe", args.topic) \
  .load()

formated_df = ds.selectExpr(
  "CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp", "timestampType"
)

formated_df.writeStream \
  .format("console") \
  .start() \
  .awaitTermination()
  
