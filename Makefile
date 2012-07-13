doc:
	cd doc/ && make html

test:
	python setup.py test

.PHONY: doc test
