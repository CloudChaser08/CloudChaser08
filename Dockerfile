##### Dewey docker container
FROM 581191604223.dkr.ecr.us-east-1.amazonaws.com/hvbase:latest
MAINTAINER HealthVerity <sysops@healthverity.com>

ENV DEBIAN_FRONTEND noninteractive
ENV HOME /root

# need repo to get updated postgres clients
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" > /etc/apt/sources.list.d/pgdg.list

# Install system reqs
RUN apt-get update &&        \
    apt-get install -y       \
     binutils                \
     jq                      \
     p7zip-full              \
     python                  \
     python-pip              \
     libpq-dev               \
     postgresql-client-9.5   \
     wget

# Clean up any files used by apt-get
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /root

# Install Python Reqs
COPY requirements.txt /root/requirements.txt
RUN pip install -r /root/requirements.txt
RUN rm /root/requirements.txt

COPY bin/run.sh     /root/run.sh
COPY bin/xls2csv.py /usr/local/bin/xls2csv.py
COPY providers      /root/providers

ENTRYPOINT ["/root/run.sh"]
