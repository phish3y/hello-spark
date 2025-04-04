#!/bin/bash
sbt package

AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT=true \
$SPARK_HOME/bin/spark-submit \
  --class "HelloSpark" \
  --master local[4] \
  --jars /home/phish3y/Downloads/hadoop-common-3.3.4.jar,/home/phish3y/Downloads/hadoop-aws-3.3.4.jar,/home/phish3y/Downloads/aws-java-sdk-bundle-1.12.782.jar \
  target/scala-2.12/hello-spark_2.12-1.0.jar
