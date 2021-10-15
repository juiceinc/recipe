========
Overview
========

.. start-badges

.. |version| image:: https://img.shields.io/pypi/v/recipe.svg
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/recipe

.. |commits-since| image:: https://img.shields.io/github/commits-since/chrisgemignani/recipe/v0.28.0.svg
    :alt: Commits since latest release
    :target: https://github.com/chrisgemignani/recipe/compare/v0.28.0...master

.. |downloads| image:: https://img.shields.io/pypi/dm/recipe.svg
    :alt: PyPI Package monthly downloads
    :target: https://pypi.python.org/pypi/recipe

.. |wheel| image:: https://img.shields.io/pypi/wheel/recipe.svg
    :alt: PyPI Wheel
    :target: https://pypi.python.org/pypi/recipe

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/recipe.svg
    :alt: Supported versions
    :target: https://pypi.python.org/pypi/recipe

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/recipe.svg
    :alt: Supported implementations
    :target: https://pypi.python.org/pypi/recipe


.. end-badges

Legos for SQL

Recipe is an MIT licensed cross-database querying library, written
in Python. It allows you to reuse SQL fragments to answer data questions
consistently. Extension classes allow you to support data anonymization,
automatic generation of where clauses, user permissioning to data, subselects,
and response formatting.

Installation
============

::

    pip install recipe

Documentation
=============

https://recipe.readthedocs.io/

Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
