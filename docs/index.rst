
Recipe: A DRY framework for querying data
=========================================

Release v\ |version|. (:ref:`Installation <install>`)

.. Contents:
..
.. .. toctree::
..    :maxdepth: 2
..

.. Indices and tables
.. ==================
..
.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`


Recipe is an :ref:`MIT Licensed <mit>` cross-database querying library, written
in Python. It allows you to reuse SQL fragments to answer build consistent data
stories. Extension classes allow you to support data anonymization, automatic
generation of where clauses, subselects, and response formatting.

.. code-block:: python

    >>> shelf = Shelf({ 'age': WtdAvgMetric(Census.age, Census.pop2000), 'state', Dimension(Census.state)})
    >>> recipe = Recipe().shelf(shelf).metrics('age').dimensions('state').order_by('-age')

    >>> recipe.to_sql()
    SELECT census.state AS state,
           CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS age
    FROM census
    GROUP BY census.state
    ORDER BY CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) DESC

    >>> recipe.dataset.csv
    state,age,state_id
    Florida,39.08283934000634,Florida
    West Virginia,38.555058651148165,West Virginia
    Maine,38.10118393261269,Maine
    Pennsylvania,38.03856695544053,Pennsylvania
    ...


User's Guide
------------

This part of the documentation, which is mostly prose, begins with some
background information about Tablib, then focuses on step-by-step instructions
for getting the most out of your datasets.

.. toctree::
   :maxdepth: 2

   intro

.. toctree::
   :maxdepth: 2

   install

.. toctree::
   :maxdepth: 3

   tutorial


API Reference
-------------

If you are looking for information on a specific function, class or
method, this part of the documentation is for you.

.. toctree::
   :maxdepth: 2

   api


Development
-----------

To add features, extensions or ingredients to Recipe see our development guide.

.. toctree::
   :maxdepth: 2

   development

