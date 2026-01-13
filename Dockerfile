FROM jupyter/pyspark-notebook:latest

USER root

RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER $NB_UID

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY drivers/mssql-jdbc-13.2.1.jre8.jar /usr/local/spark/jars/

ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3

WORKDIR /home/jovyan/work

EXPOSE 8888 4040

