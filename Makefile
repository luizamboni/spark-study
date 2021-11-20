up:
	docker-compose up

run-kafka-spark-job:
	docker-compose run --rm glue \
		spark-submit \
		--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 \
		/home/project/stream/spark/kafka_dataframe.py \
		zookeeper 2181 foobar

attach-utils:
	docker-compose run --rm utils /bin/bash

attach-glue:
	docker-compose run --rm glue /bin/bash

run-kafka-produce:
	docker-compose run --rm utils python3 examples/stream/produce.py
