.. _development:

Development
===========

Recipe is under active development, and contributors are welcome.

If you have a feature request, suggestion, or bug report, please open a new
issue on GitHub_. To submit patches, please send a pull request on GitHub_.

.. _GitHub: http://github.com/juiceinc/recipe/



.. _design:

-----------
Conventions
-----------

Recipe code wraps at 79 characters and passes flake8. Strings should single
quoted unless double quoting leads to less escaping. Add tests to achieve
100% code coverage.


.. _scm:

--------------
Source Control
--------------

The project is hosted on at https://github.com/juiceinc/recipe

The repository is publicly accessible. To check it out, run:

    ``git clone git://github.com/juiceinc/recipe.git``



Git Branch Structure
++++++++++++++++++++


``develop``
    The "next release" branch. Likely unstable.
``master``
    Current production release (|version|) on PyPi.

Each release is tagged.

When submitting patches, please place your feature/change in its own branch
prior to opening a pull request on GitHub_.



.. _newextensions:

---------------------
Adding New Extensions
---------------------

Recipe welcomes new extensions.


Building Extensions
~~~~~~~~~~~~~~~~~~~

Extensions subclass RecipeExtension and plug into the base recipe's ``.query()``
method which builds a SQLAlchemy query. Extensions can either modify the base
recipe like these do.

* AutomaticFilters
* Anonymize
* SummarizeOver


Or extensions can merge one or more recipes into the base recipe. Extensions
that require another recipe should have a classname that ends with **Recipe**.

* CompareRecipe
* BlendRecipe

When adding an extension, do the following.

1) Add extension to src/extensions.py
2) Add tests to tests/test_extensions.py, cover 100% of extension function
   and test that the extension doesn't interfere with other extensions
3) Make sure your extension code passes flake8
4) Add extension description to docs/extensions/
5) Submit a PR!

----------------------
Adding New Ingredients
----------------------

Recipe welcomes new ingredients, particularly metrics and dimensions that
cover common patterns of data aggregation.


Building Ingredients
~~~~~~~~~~~~~~~~~~~~

Subclass the appropriate ingredient and don't duplicate something that a
superclass does. For instance ``WtdAvgMetric`` is a subclass of
``DivideMetric`` that generates it's expressions differently.

Extra functionality can be added by using Ingredient.meta in structured ways.

A checklist of adding an extension.

1) Add extension to src/ingredients.py
2) Add tests to tests/test_ingredients.py, cover 100% of ingredient
   parameters.
3) Make sure your ingredient passes flake8
4) Submit a PR!


.. _testing:

--------------
Testing Recipe
--------------

Testing is crucial to confident development and stability. This stable
project is used in production by many companies and developers, so it is
important to be certain that every version released is fully operational.
When developing a new feature for Recipe, be sure to write proper tests for it
as well.

When developing a feature for Recipe, the easiest way to test your changes for
potential issues is to simply run the test suite directly. ::

	$ make tests

This will run tests under pytest and show code coverage data.



.. _jenkins:

----------------------
Continuous Integration
----------------------

Every commit made to the **develop** branch is automatically tested and
inspected upon receipt with `Travis CI`_. If you have access to the main
repository and broke the build, you will receive an email accordingly.

Anyone may view the build status and history at any time.

    https://travis-ci.org/juiceinc/tablib

Additional reports will also be included here in the future, including :pep:`8`
        checks and stress reports for extremely large datasets.

.. _`Jenkins CI`: https://travis-ci.org/


.. _docs:

-----------------
Building the Docs
-----------------

Documentation in `reStructured Text`_ and powered by Sphinx_.

The Docs live in ``recipe/docs``. In order to build them, you will first need
 to install Sphinx. ::

	$ pip install sphinx


To build an HTML version of the docs, simply run the following from the
**docs** directory: ::

	$ make html

Your ``docs/_build/html`` directory will then contain the fully build
documentation, ready for publishing. You can also generate the documentation
in tons of other formats.

.. _`reStructured Text`: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx.pocoo.org

----------

If you want to learn more, check out the :ref:`API Documentation <api>`.
