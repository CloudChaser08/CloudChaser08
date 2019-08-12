##### Dewey docker container
FROM 581191604223.dkr.ecr.us-east-1.amazonaws.com/hvbase:hv-18.04-latest
MAINTAINER HealthVerity <sysops@healthverity.com>

ARG SPARK_VERSION=2.2.1

ENV DEBIAN_FRONTEND noninteractive
ENV HOME /root

RUN apt-get update && \
    apt-get install -y \
     python \
     python-pip \
     python3-pip \
     zip \
     openjdk-8-jre-headless \
     curl \
     libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN cd /usr/lib/ && \
     curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
     > spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && \
     tar -xzf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && \
     mv spark-${SPARK_VERSION}-bin-hadoop2.7 spark && \
     rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

COPY etc/dev_requirements.txt /dev_requirements.txt

RUN python -m pip install -r /dev_requirements.txt --ignore-installed
RUN python3.6 -m pip install -r /dev_requirements.txt --ignore-installed

ENV SPARK_HOME /usr/lib/spark
ENV PYTHONPATH $SPARK_HOME/python/build
ENV PYTHONPATH $SPARK_HOME/python/:$PYTHONPATH
ENV PYTHONPATH $SPARK_HOME/python/lib:$PYTHONPATH
ENV PYTHONPATH $SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

WORKDIR /root/spark

ENTRYPOINT ["/bin/bash"]
