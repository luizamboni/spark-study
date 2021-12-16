
# this versions is constrainerd by glue image
export SPARK_VERSION=2.4.0
export SCALA_VERSION=2.11


up:
	docker-compose up -d zookeeper kafka-admin kafka

run-kafka-spark-job-dstream:
	docker-compose run --rm glue \
		spark-submit \
		--packages org.apache.spark:spark-streaming-kafka-0-8_$$SCALA_VERSION:$$SPARK_VERSION \
		/home/project/examples/stream/spark/kafka_with_dstreams_api.py \
		--host=zookeeper:2181 \
		--topic=foobar

run-kafka-clean-checkpoint:
	docker-compose run --rm glue rm -rf /home/project/examples/stream/spark/checkpoints/*

run-kafka-spark-job-df-api:
	docker-compose run --rm glue \
		spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_$$SCALA_VERSION:$$SPARK_VERSION \
		/home/project/examples/stream/spark/kafka_with_dataframe_api.py \
		--host=kafka:9092 \
		--topic=foobar

attach-utils:
	docker-compose run --rm utils /bin/bash

attach-glue:
	docker-compose run --rm glue /bin/bash

run-kafka-produce:
	docker-compose run --rm utils \
	python3 examples/stream/python/kafka-producer.py \
	--host=kafka:9092 \
	--topic=foobar \
	--volume=10 \
	--infinity=true

run-kafka-setup:
	docker-compose run --rm utils \
	python3 examples/stream/python/kafka-setup.py \
	--host=kafka:9092 \
	--topic=foobar

run-kafka-consumer:
	docker-compose run --rm utils \
	python3 examples/stream/python/kafka-consumer.py \
	--host=kafka:9092 \
	--topic=foobar
