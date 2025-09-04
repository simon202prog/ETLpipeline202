FROM jupyter/pyspark-notebook:spark-3.3.2

SHELL ["/bin/bash", "-c"]

USER root
# Install optional Python packages commonly used with Spark
RUN pip install --no-cache-dir jupyterlab kafka-python==2.0.2 delta-spark==2.2.0 boto3 awscli

# Add Hadoop AWS and AWS SDK jars for S3A support (match Hadoop 3.3.x)
ENV SPARK_HOME=/usr/local/spark
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -O $SPARK_HOME/jars/hadoop-aws-3.3.2.jar \
 && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar

# Add ClickHouse JDBC driver for writing via JDBC from Spark
RUN wget -q https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6-patch11/clickhouse-jdbc-0.4.6-patch11-all.jar -O $SPARK_HOME/jars/clickhouse-jdbc-all.jar || \
    wget -q https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar -O $SPARK_HOME/jars/clickhouse-jdbc-all.jar

USER $NB_UID

WORKDIR /home/jovyan/work

EXPOSE 8888 4040

# Start Jupyter without token/password for local development
CMD ["start-notebook.sh", "--NotebookApp.token=", "--NotebookApp.password="]