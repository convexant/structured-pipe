FROM gettyimages/spark

MAINTAINER Sam BESSALAH <samklr@me.com>

ENV JOB_BINARY /opt/streaming_aggregation_poc-assembly-1.0.jar

COPY target/scala-2.11/streaming_aggregation_poc-assembly-1.0.jar /opt

COPY run_job.sh /opt
