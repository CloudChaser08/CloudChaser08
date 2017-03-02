FROM ubuntu:14.04

# Airflow
ARG AIRFLOW_VERSION=1.7.1.3
ENV AIRFLOW_HOME /usr/local/airflow

RUN apt-get update
RUN apt-get install -y software-properties-common

RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN add-apt-repository ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN apt-get install -q -y oracle-java7-installer
RUN apt-get install -q -y oracle-java7-set-default

ENV JAVA_HOME /usr/lib/jvm/java-7-oracle/

ADD bin/UnlimitedJCEPolicyJDK8/local_policy.jar ${JAVA_HOME}/jre/lib/security/
ADD bin/UnlimitedJCEPolicyJDK8/US_export_policy.jar ${JAVA_HOME}/jre/lib/security/

RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        postgresql-9.5             \
        postgresql-client-9.5      \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        python-pip \
        apt-utils \
        unzip \
        curl \
        netcat \
        jq \
        sqlite3 \
        libpq-dev

RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

RUN pip install -U pip setuptools

RUN pip install \
  pandas==0.19.2 \
  numpy==1.12.0

COPY etc/requirements.txt /
RUN pip install -r /requirements.txt

RUN apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY bin/entrypoint.sh /entrypoint.sh
COPY etc/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
