#!/usr/bin/env bash

spark-submit --class com.the_ica. ....StreamingJob  \
             --master $SPARK_MASTER  \
             --executor-memory 2G   \
             --total-executor-cores 2 \
             --supervise $JOB_BINARY

