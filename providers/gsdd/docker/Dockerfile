FROM centos:7
MAINTAINER HealthVerity <sysops@healthverity.com>

ENV HOME /root
ENV SHELL /bin/bash

# this now auto-unpacks
ADD resource/GSDDServer-5.12.1.711-CentOS7.6-64bit.tar.gz /opt

WORKDIR /opt
RUN rpm -iUvh https://dl.fedoraproject.org/pub/epel/7/x86_64/Packages/e/epel-release-7-13.noarch.rpm
RUN yum install -y man git install python-pip make compat-libstdc++-33.i686 openssl-libs.x86_64 curl zip unzip
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

WORKDIR GSDDServer-5.12.1.711-CentOS7.6-64bit/

ADD resource/GSDD.lic /opt/goldstandard/gsdd5/
ADD bin/gsdd.sh /usr/local/bin/
ADD resource/install_input /root/
RUN echo -e "https://update.gsdd.net/UpdateService4/UpdateService.asmx\n" > /opt/goldstandard/gsdd5/gsddupdate.endpoint


RUN mkdir /root/.ssh
RUN echo -e "Host github.com\n\tStrictHostKeyChecking no\n" > /root/.ssh/config


RUN ./gsdd-install.pl < /root/install_input
CMD /usr/local/bin/gsdd.sh
