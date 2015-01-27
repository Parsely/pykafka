FROM debian:jessie
MAINTAINER Yung-Chin Oei <yungchin@yungchin.nl>

RUN apt-get -y update

RUN apt-get -y install python-dev python-setuptools python-snappy ipython
RUN ln -s /srv/pykafka/.ipython /root/.ipython
