APACHE_MIRROR = http://mirrors.sonic.net/apache

KAFKA_VERSION := 0.7.2-incubating
KAFKA_FULL = kafka-$(KAFKA_VERSION)-src
KAFKA_URL = http://archive.apache.org/dist/kafka/old_releases/kafka-$(KAFKA_VERSION)/kafka-$(KAFKA_VERSION)-src.tgz
KAFKA_SRC_TGZ = $(notdir $(KAFKA_URL))

ZOOKEEPER_VERSION := 3.3.6
ZOOKEEPER_FULL = zookeeper-$(ZOOKEEPER_VERSION)
ZOOKEEPER_URL = $(APACHE_MIRROR)/zookeeper/$(ZOOKEEPER_FULL)/$(ZOOKEEPER_FULL).tar.gz
ZOOKEEPER_TGZ = $(notdir $(ZOOKEEPER_URL))

all: kafka zookeeper

clean:
	rm -r kafka zookeeper

kafka:
	curl -O $(KAFKA_URL)
	tar xzf $(KAFKA_SRC_TGZ)
	cd $(KAFKA_FULL) \
		&& ./sbt update \
		&& ./sbt package
	cd ..
	mv $(KAFKA_FULL) kafka

zookeeper:
	curl -O $(ZOOKEEPER_URL)
	tar xvf $(ZOOKEEPER_TGZ)
	mv $(ZOOKEEPER_FULL) zookeeper

.PHONY: clean
