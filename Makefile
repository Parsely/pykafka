doc:
	pip install pykafka[docs]
	cd doc/ && make html

lint:
	pip install --use-mirrors kafka[lint]
	pyflakes ./kafka
	pyflakes ./tests
	pep8 ./kafka ./tests

unit:
	python setup.py nosetests --attr=!integration

integration:
	python setup.py nosetests --attr=integration

test:
	python setup.py nosetests

vendor:
	make -C vendor


.PHONY: doc unit integration test lint vendor
