APACHE_MIRROR = http://mirrors.sonic.net/apache

KAFKA_VERSION := 0.7.1-incubating
KAFKA_FULL = kafka-$(KAFKA_VERSION)
KAFKA_URL = $(APACHE_MIRROR)/incubator/kafka/kafka-$(KAFKA_VERSION)/kafka-$(KAFKA_VERSION)-src.tgz
KAFKA_SRC_TGZ = $(notdir $(KAFKA_URL))

ZOOKEEPER_VERSION := 3.3.5
ZOOKEEPER_FULL = zookeeper-$(ZOOKEEPER_VERSION)
ZOOKEEPER_URL = $(APACHE_MIRROR)/zookeeper/$(ZOOKEEPER_FULL)/$(ZOOKEEPER_FULL).tar.gz
ZOOKEEPER_TGZ = $(notdir $(ZOOKEEPER_URL))

all: kafka zookeeper

clean:
	rm -r kafka zookeeper

kafka:
	curl -O $(KAFKA_URL)
	tar xzf $(KAFKA_SRC_TGZ)
	cd kafka-$(KAFKA_VERSION) \
		&& ./sbt update \
		&& ./sbt package
	cd ..
	mv $(KAFKA_FULL) kafka

zookeeper:
	curl -O $(ZOOKEEPER_URL)
	tar xvf $(ZOOKEEPER_TGZ)
	mv $(ZOOKEEPER_FULL) zookeeper

.PHONY: clean
