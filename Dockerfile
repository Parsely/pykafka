FROM debian:jessie
MAINTAINER Yung-Chin Oei <yungchin@yungchin.nl>

RUN apt-get -y update

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# dependencies for testing:
RUN apt-get -y install build-essential curl openjdk-7-jdk
COPY Makefile /usr/src/app/
COPY vendor /usr/src/app/vendor/
RUN make vendor
ENV ZOOKEEPER_PATH /usr/src/app/vendor/zookeeper
ENV KAFKA_PATH /usr/src/app/vendor/kafka

RUN apt-get -y install python-dev python-setuptools python-snappy ipython
RUN ln -s /usr/src/app/.ipython /root/.ipython
