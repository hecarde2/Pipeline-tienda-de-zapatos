FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        ca-certificates \
        procps \
        tini \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Jars de Spark: conector Kafka, driver JDBC de PostgreSQL y soporte S3 (Hadoop AWS)
RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-4/zstd-jni-1.5.5-4.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar

COPY app ./app
COPY dashboard ./dashboard
COPY db ./db
# Tema de Streamlit (blanco hueso) para el dashboard
COPY streamlit-config.toml /app/.streamlit/config.toml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Usuario no root con el mismo uid que el anfitrión para no ensuciar ./data
RUN groupadd -g 1001 app && useradd -m -u 1001 -g 1001 app \
    && mkdir -p /data && chown -R app:app /app /data

USER app
ENV HOME=/home/app \
    LAKE_ROOT=/data/lake \
    RAW_ROOT=/data/raw \
    SPARK_DRIVER_MEMORY=768m \
    PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "-m", "app.batch"]
