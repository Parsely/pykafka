FROM debian:jessie
MAINTAINER Yung-Chin Oei <yungchin@yungchin.nl>

RUN apt-get -y update

# dependencies for testing:
RUN apt-get -y install build-essential curl openjdk-7-jdk
COPY vendor /srv/vendor/
WORKDIR /srv/vendor/
RUN make all
ENV ZOOKEEPER_PATH /srv/vendor/zookeeper
ENV KAFKA_PATH /srv/vendor/kafka

RUN apt-get -y install python-dev python-setuptools python-snappy ipython
RUN ln -s /srv/pykafka/.ipython /root/.ipython
