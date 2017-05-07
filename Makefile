.PHONY: all docs tests .FORCE

all: docs flake8 tests

docs:
	cd docs && make html

autodocs:
	sphinx-autobuild docs docs/_build/html/ --port 8001

flake8:
	flake8 src/recipe --exit-zero --max-complexity 12 --exclude=__init__.py

tests:
	py.test --cov-config .coveragerc --cov=recipe tests/
