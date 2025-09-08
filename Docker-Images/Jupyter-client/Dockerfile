FROM jupyter/pyspark-notebook:spark-3.3.2

USER root

# Устанавливаем дополнительные Python пакеты
RUN pip install --no-cache-dir jupyterlab kafka-python==2.0.2 delta-spark==2.2.0 boto3 awscli

# Устанавливаем Hadoop AWS jars для работы с S3A
ENV SPARK_HOME=/usr/local/spark
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -O $SPARK_HOME/jars/hadoop-aws-3.3.2.jar \
 && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar

USER $NB_UID
WORKDIR /home/jovyan/work

EXPOSE 8888 4040

CMD ["start-notebook.sh", "--NotebookApp.token=", "--NotebookApp.password="]
