#!/bin/bash
sbt package

aws s3 cp target/scala-2.12/hello-spark_2.12-1.0.jar s3://phish3y-hello-spark/jars/hello-spark_2.12-1.0.jar

kubectl delete -f infra/spark-app-stream-subscriber.yaml --ignore-not-found
kubectl apply -f infra/spark-app-stream-subscriber.yaml
