KAFKA_VERSION := 0.7.1-incubating
KAFKA_FULL = kafka-$(KAFKA_VERSION)
KAFKA_URL = http://mirrors.sonic.net/apache/incubator/kafka/kafka-$(KAFKA_VERSION)/kafka-$(KAFKA_VERSION)-src.tgz
KAFKA_SRC_TGZ = $(notdir $(KAFKA_URL))

doc:
	pip install samsa[docs]
	cd doc/ && make html

lint:
	pip install --use-mirrors samsa[lint]
	pyflakes ./samsa
	pyflakes ./tests
	pep8 ./samsa ./tests

unit:
	python setup.py nosetests --attr=!integration

integration:
	python setup.py nosetests --attr=integration

test:
	python setup.py nosetests

$(KAFKA_SRC_TGZ):
	curl -O $(KAFKA_URL)

$(KAFKA_FULL): $(KAFKA_SRC_TGZ)
	tar xzf $(KAFKA_SRC_TGZ)

kafka: $(KAFKA_FULL)
	cd kafka-$(KAFKA_VERSION) \
		&& ./sbt update \
		&& ./sbt package
	cd ..
	mv $(KAFKA_FULL) kafka


.PHONY: doc unit integration test lint
