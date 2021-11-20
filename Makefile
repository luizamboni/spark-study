
# this versions is constrainerd by glue image
export SPARK_VERSION=2.4.0
export SCALA_VERSION=2.11


up:
	docker-compose up -d

run-kafka-spark-job:
	docker-compose run --rm glue \
		spark-submit \
		--packages org.apache.spark:spark-streaming-kafka-0-8_$$SCALA_VERSION:$$SPARK_VERSION \
		/home/project/stream/spark/kafka_dataframe.py \
		zookeeper 2181 foobar

run-kafka-spark-job-with-datasource-lib:
	docker-compose run --rm glue \
		spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_$$SCALA_VERSION:$$SPARK_VERSION \
		/home/project/stream/spark/kafka_dataframe_streaming_queries.py \
		kafka 9092 foobar

attach-utils:
	docker-compose run --rm utils /bin/bash

attach-glue:
	docker-compose run --rm glue /bin/bash

run-kafka-produce:
	docker-compose run --rm utils python3 examples/stream/produce.py



# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 /home/project/stream/spark/kafka_dataframe_2.py zookeeper 2181 foobar
