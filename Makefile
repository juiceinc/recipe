.PHONY: all docs tests .FORCE

all: docs flake8 tests

docs:
	cd docs && make html

flake8:
	flake8 src/recipe --exit-zero --exclude=__init__.py

tests:
	py.test --cov-config .coveragerc --cov=recipe tests/
