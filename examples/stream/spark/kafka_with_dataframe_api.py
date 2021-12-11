
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

def print_df(df, outputMode=None):

  if df.isStreaming:
      df.writeStream \
        .format("console") \
        .outputMode(outputMode) \
        .option('truncate', 'false') \
        .start()
  else:
    df.show(truncate= False)
    return df


def process_stream(rd, once = False, processingTime = False, continuous = False, windowTime = None, slideTime = None):
  print_df(rd, "append")

  formated_df = rd.selectExpr(
    "CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp", "timestampType"
  )

  # ## complete output mode
  grouped_by_key_df = formated_df.groupBy('key') \
                          .count()

  print_df(grouped_by_key_df,"complete")

  ## append with whatermark
  windowGroup =  window(col('timestamp'), windowTime , slideTime) if slideTime else window(col('timestamp'), windowTime )
  grouped_by_window_and_key_df = formated_df.withWatermark("timestamp", "2 seconds") \
                          .groupBy(
                            windowGroup,
                            col('key')
                          ) \
                          .count()

  print_df(grouped_by_window_and_key_df, "append")

  if formated_df.isStreaming:
    writeStream = formated_df.writeStream
                          
    # comment and uncomment to use other trigger strategies
    if processingTime:
      writeStream = writeStream.trigger(processingTime=processingTime)
    elif once:
      writeStream = writeStream.trigger(once=True)
    elif continuous:
      writeStream = writeStream.trigger(continuous=continuous)
    else:
      pass
  
    formated_df.writeStream.option(
      "checkpointLocation", 
      "file:////home/project/examples/stream/spark/_checkpoints/a",
    ) \
    .format("console") \
    .start() \
    .awaitTermination()
  else:
    formated_df.show()

  return (
    formated_df,
    grouped_by_key_df,
    grouped_by_window_and_key_df,
  )
  
if __name__ == "__main__":

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

  ds.printSchema()

  process_stream(ds, once=True, windowTime="2 seconds", slideTime="1 seconds")