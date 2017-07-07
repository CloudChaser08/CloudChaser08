FROM centos:7
MAINTAINER HealthVerity <sysops@healthverity.com>

ENV HOME /root
ENV SHELL /bin/bash

# this now auto-unpacks
ADD resource/GSDDServer-5.4.0.546-CentOS6.6.tar.gz /opt

WORKDIR /opt
RUN rpm -iUvh https://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-9.noarch.rpm 
RUN yum install -y man git install python-pip make compat-libstdc++-33.i686 openssl-libs.i686
RUN pip install awscli

WORKDIR GSDDServer-5.4.0.546-CentOS6.6/

ADD resource/GSDD.lic /opt/goldstandard/gsdd5/
ADD bin/gsdd.sh /usr/local/bin/

RUN mkdir /root/.ssh
RUN echo -e "Host github.com\n\tStrictHostKeyChecking no\n" > /root/.ssh/config


RUN ./gsdd-install.pl
CMD /usr/local/bin/gsdd.sh
