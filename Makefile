
# this versions is constrainerd by glue image
export SPARK_VERSION=3.1.2
export SCALA_VERSION=2.12

include .env

up:
	docker-compose up -d zookeeper kafka-admin kafka

run-kafka-clean-checkpoint:
	docker-compose run --rm spark \
	rm -rf  /opt/bitnami/spark/jobs/stream/spark/_checkpoints  /opt/bitnami/spark/jobs/stream/spark/_checkpoint

run-kafka-spark-job-df-api:
	docker-compose run --rm spark \
		spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION} \
		/opt/bitnami/spark/jobs/stream/spark/kafka_with_dataframe_api.py \
		--host=kafka:9092 \
		--topic=foobar

attach-utils:
	docker-compose run --rm utils /bin/bash


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
	--topic=foobar \
	--group_id=teste2 \
	--length=10