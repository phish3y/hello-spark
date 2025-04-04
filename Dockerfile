FROM spark:3.5.5

USER root

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar $SPARK_HOME/jars 
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.782/aws-java-sdk-bundle-1.12.782.jar $SPARK_HOME/jars

RUN chmod 644 $SPARK_HOME/jars/hadoop-common-3.3.4.jar
RUN chmod 644 $SPARK_HOME/jars/hadoop-aws-3.3.4.jar
RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.782.jar

ENTRYPOINT ["/opt/entrypoint.sh"]