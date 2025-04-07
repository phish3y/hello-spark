#!/bin/bash
sbt package

AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT=true KAFKA_BROKER=192.168.0.132:9094 \
$SPARK_HOME/bin/spark-submit \
  --class "HelloSparkStreamProducer" \
  --master local[4] \
  target/scala-2.12/hello-spark_2.12-1.0.jar
