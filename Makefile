up-container:
	docker pull amazon/aws-glue-libs:glue_libs_1.0.0_image_01 && \
	docker run -itd \
		-p 8888:8888 \
		-p 4040:4040 \
		-v ~/.aws:/root/.aws:ro \
		-v $(shell pwd)/examples/:/home/project \
		--name glue amazon/aws-glue-libs:glue_libs_1.0.0_image_01

down-container:
	docker stop glue && docker rm glue

attach:
	docker exec \
	-w /home/project/ \
	-e PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/share/maven/bin:/home/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8/bin/" \
	-it glue \
	/bin/bash

