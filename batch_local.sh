#!/bin/bash
sbt package

AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT=true \
$SPARK_HOME/bin/spark-submit \
  --class "HelloSparkBatch" \
  --master local[4] \
  target/scala-2.12/hello-spark_2.12-1.0.jar
