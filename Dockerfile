FROM yungchin/python-librdkafka:0.0.0.dev0

RUN apt-get -y install python-pip python-snappy
RUN pip install -i https://testpypi.python.org/pypi rd_kafka==0.0.0.dev0
