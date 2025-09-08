FROM apache/airflow:2.8.0

USER root
# Скопируем requirements
COPY Airflow-image/requirements.txt /requirements.txt

# Ставим java, ant и чистим apt
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Настраиваем JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# переключаемся обратно на airflow перед pip
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
