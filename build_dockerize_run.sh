#! /bin/bash

sbt clean assembly

sudo docker build -t streamjob .

sudo docker run -it  -e SPARK_MASTER="spark://localhost:7077" \
                     -e KAFKA_HOSTS="localhost:9092" \
                     -e CASSANDRA_SEEDS="localhost" \
                     --net=host \
                     streamjob  \
                     /opt/run_job.sh
