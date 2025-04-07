FROM spark:3.5.5

USER root

# Add support for S3 and Kafka
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar $SPARK_HOME/jars 
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.782/aws-java-sdk-bundle-1.12.782.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar  $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar $SPARK_HOME/jars

RUN chmod 644 $SPARK_HOME/jars/hadoop-common-3.3.4.jar
RUN chmod 644 $SPARK_HOME/jars/hadoop-aws-3.3.4.jar
RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.782.jar
RUN chmod 644 $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar
RUN chmod 644 $SPARK_HOME/jars/commons-pool2-2.12.1.jar
RUN chmod 644 $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar
RUN chmod 644 $SPARK_HOME/jars/kafka-clients-3.5.2.jar

ENTRYPOINT ["/opt/entrypoint.sh"]