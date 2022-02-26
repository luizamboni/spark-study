FROM bitnami/spark:3.1.2
USER root

RUN pip install boto3
ENV PYTHONPATH=/opt/bitnami/spark/python/:/opt/bitnami/spark/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH

COPY ./examples/ /opt/bitnami/spark/jobs/

USER 1001

CMD [ "bash" ]