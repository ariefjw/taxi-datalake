FROM apache/airflow:2.7.1
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir dbt-postgres requests pandas pyarrow fastparquet sqlalchemy psycopg2-binary pyspark wget