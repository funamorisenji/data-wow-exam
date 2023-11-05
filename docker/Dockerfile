FROM apache/airflow

ARG SPARK_VERSION="3.5.0"
ARG HADOOP_VERSION="3"

USER root

# JAVA installation, Python installation, and essential packages
RUN apt update && \
    apt-get install -y \
    wget unzip zip openjdk-11-jdk ant git gcc procps && \
    apt-get clean

#
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64
RUN export JAVA_HOME


# Spark installation
ENV SPARK_HOME /usr/local/spark
RUN wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -P /tmp && \
    tar -xvzf "/tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /tmp && \
    mkdir -p "${SPARK_HOME}/bin" "${SPARK_HOME}/assembly/target/scala-2.13/jars" && \
    cp -a "/tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
    cp -a "/tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.13/jars/" && \
    rm "/tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

ENV PATH $PATH:/usr/local/spark/bin

#USER airflow
#
#RUN airflow db init

# Python packages installation
USER airflow

RUN airflow db init
RUN pip install apache-airflow-providers-apache-spark loguru pendulum

COPY ./airflow_start.sh /usr/bin/airflow_start.sh
#COPY ./Volumes/spark/jars /usr/local/spark/jars
COPY ./Volumes/spark/jars /usr/local/spark/assembly/target/scala-2.13/jars/
ENTRYPOINT [ "/usr/bin/airflow_start.sh" ]
