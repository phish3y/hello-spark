SBT := sbt
SPARK_HOME := /home/phish3y/dev/spark-3.5.5-bin-hadoop3
AWS := aws
KUBECTL := kubectl

.PHONY: fmt
fmt:
	$(SBT) scalafmtAll

.PHONY: build
build: fmt
	$(SBT) package

.PHONY: deploy
deploy: build
	$(AWS) s3 cp target/scala-2.12/hello-spark_2.12-1.0.jar s3://phish3y-hello-spark/jars/hello-spark_2.12-1.0.jar

.PHONY: hut-batch-local
hut-batch-local: build
	AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT=true \
	$(SPARK_HOME)/bin/spark-submit \
		--class "HutBatch" \
		--master local[4] \
		target/scala-2.12/hello-spark_2.12-1.0.jar

.PHONY: hut-batch-cluster
hut-batch-cluster: deploy
	$(KUBECTL) delete -f infra/batch-hut.yaml --ignore-not-found
	$(KUBECTL) apply -f infra/batch-hut.yaml

.PHONY: hut-stream-producer-local
hut-stream-producer-local: build
	AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT=true KAFKA_BROKER=192.168.0.132:9094 \
	$(SPARK_HOME)/bin/spark-submit \
		--class "HutProducer" \
		--master local[4] \
		target/scala-2.12/hello-spark_2.12-1.0.jar

.PHONY: hut-stream-producer-cluster
hut-stream-producer-cluster: deploy
	$(KUBECTL) delete -f infra/hut-stream-producer.yaml --ignore-not-found
	$(KUBECTL) apply -f infra/hut-stream-producer.yaml

.PHONY: hut-stream-subscriber-local
hut-stream-subscriber-local: build
	AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT=true KAFKA_BROKER=192.168.0.132:9094 \
	(SPARK_HOME)/bin/spark-submit \
		--class "HutSubscriber" \
		--master local[4] \
		target/scala-2.12/hello-spark_2.12-1.0.jar

.PHONY: hut-stream-subscriber-cluster
hut-stream-subscriber-cluster: build
	$(KUBECTL) delete -f infra/hut-stream-subscriber.yaml --ignore-not-found
	$(KUBECTL) apply -f infra/hut-stream-subscriber.yaml
