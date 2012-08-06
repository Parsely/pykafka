doc:
	cd doc/ && make html

lint:
	pip install -r requirements/lint.txt
	pyflakes ./samsa
	pyflakes ./tests
	pep8 ./samsa ./tests

unit:
	python setup.py nosetests --attr=!integration

integration:
	python setup.py nosetests --attr=integration

test:
	python setup.py test

.PHONY: doc unit integration test lint
