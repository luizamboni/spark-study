import os
import argparse
from pyspark.sql import SparkSession
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

def print_df(df, outputMode=None, once=False, processingTime=False, continuous=False, checkpoint = False, awaitTermination = False):

  if df.isStreaming:
      writeStream = df.writeStream
      if processingTime:
        writeStream = writeStream.trigger(processingTime=processingTime)
      elif once:
        writeStream = writeStream.trigger(once=True)
      elif continuous:
        writeStream = writeStream.trigger(continuous=continuous)

      if checkpoint: 
        writeStream = writeStream.option(
          "checkpointLocation", 
          checkpoint,
        )

      writeStream = writeStream \
        .format("console") \
        .outputMode(outputMode) \
        .option('truncate', 'false')

      if awaitTermination:
        return writeStream.start().awaitTermination()
      else:
        return writeStream.start()

  else:
    df.show(truncate= False)
    return df


def process_stream(rd, once = False, processingTime = False, continuous = False, windowTime = None, slideTime = None, checkpointDir = None):
  print_df(rd, "append", once, processingTime, continuous, checkpoint=f"{checkpointDir}/original", awaitTermination=False)
  
  grouped_by_key_df = None
  grouped_by_window_and_key_df = None
  
  formated_df = rd.selectExpr(
    "CAST(key AS STRING)", 
    "CAST(value AS STRING)", 
    "topic", 
    "partition", 
    "offset", 
    "timestamp", 
    "timestampType"
  )

  # # ## complete output mode
  # if slideTime:
  # grouped_by_key_df = formated_df.groupBy('key') \
  #                         .count()

  # print_df(grouped_by_key_df, "complete", once, processingTime, continuous)

  # ## append with whatermark
  # windowGroup = window(col('timestamp'), windowTime , slideTime) if slideTime else window(col('timestamp'), windowTime )
  # grouped_by_window_and_key_df = formated_df.withWatermark("timestamp", "2 seconds") \
  #                         .groupBy(
  #                           windowGroup,
  #                           col('key')
  #                         ) \
  #                         .count()


  print_df(formated_df, "append", once, processingTime, continuous, checkpoint=f"{checkpointDir}/a", awaitTermination=True)

  return (
    formated_df,
    grouped_by_key_df,
    grouped_by_window_and_key_df,
  )
  
if __name__ == "__main__":

  print(__file__)
  checkpointDir = f"file:///{os.path.dirname(__file__)}/_checkpoint"

  args = get_args()

  spark = SparkSession.builder.appName("streamReader").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  ds = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", args.host) \
    .option("subscribe", args.topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()

  # ds.printSchema()

  process_stream(ds, once=True, windowTime="2 seconds", slideTime="1 seconds", checkpointDir = checkpointDir)