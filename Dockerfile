FROM debian:jessie
MAINTAINER Yung-Chin Oei <yungchin@yungchin.nl>

RUN apt-get -y update

RUN apt-get -y install ipython # optional of course
RUN ln -s /srv/pykafka/.ipython /root/.ipython

RUN apt-get -y install python-dev python-pip python-setuptools

# pykafka dependencies:
RUN apt-get -y install python-snappy

# rd_kafka dependencies:
RUN apt-get -y install librdkafka-dev gcc python-cffi
RUN pip install -i https://testpypi.python.org/pypi rd_kafka==0.0.0.dev0
