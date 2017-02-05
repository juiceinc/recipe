========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |codecov|
    * - package
      - | |version| |downloads| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|

.. |docs| image:: https://readthedocs.org/projects/recipe/badge/?style=flat
    :target: https://readthedocs.org/projects/recipe
    :alt: Documentation Status

.. |travis| image:: https://travis-ci.org/chrisgemignani/recipe.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/chrisgemignani/recipe

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/chrisgemignani/recipe?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/chrisgemignani/recipe

.. |requires| image:: https://requires.io/github/chrisgemignani/recipe/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/chrisgemignani/recipe/requirements/?branch=master

.. |codecov| image:: https://codecov.io/github/chrisgemignani/recipe/coverage.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/chrisgemignani/recipe

.. |version| image:: https://img.shields.io/pypi/v/recipe.svg
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/recipe

.. |commits-since| image:: https://img.shields.io/github/commits-since/chrisgemignani/recipe/v0.1.0.svg
    :alt: Commits since latest release
    :target: https://github.com/chrisgemignani/recipe/compare/v0.1.0...master

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

* Free software: BSD license

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
