
from pyspark.sql import SparkSession
import sys
if len(sys.argv) != 4:
  print(
    "Usage: *.py <hostname> <port>", 
    file=sys.stderr
  )
  sys.exit(-1)

host = sys.argv[1]
port = int(sys.argv[2])
topic = sys.argv[3]

host_and_port= f"{host}:{port}"
spark = SparkSession.builder.appName("streamReader").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print(host_and_port, topic)

ds = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", host_and_port) \
  .option("subscribe", topic) \
  .load()

# df = ds.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

write_stream = ds.writeStream.format("console").start()
write_stream.awaitTermination()