
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


Recipe is an MIT licensed cross-database querying library, written
in Python. It allows you to reuse SQL fragments to answer data questions
consistently. Extension classes allow you to support data anonymization,
automatic generation of where clauses, user permissioning to data, subselects,
and response formatting.

.. code-block:: python

    >>> shelf = Shelf({ 'age': WtdAvgMetric(Census.age, Census.pop2000), 'state': Dimension(Census.state)})
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

.. toctree::
   :caption: Getting started
   :maxdepth: 2

   intro
   install
   tutorial

.. toctree::
   :caption: Key Concepts
   :maxdepth: 3

   concepts/overview
   concepts/ingredients
   concepts/shelves
   concepts/from_field_expression_config
   concepts/from_config
   concepts/using_from_config
   extensions/intro_to_extensions

.. toctree::
   :caption: Advanced Topics
   :maxdepth: 3

   advanced/settings
   advanced/ovens
   advanced/hooks

.. toctree::
   :caption: API Reference
   :maxdepth: 2

   api

.. toctree::
   :caption: Contributing to Development
   :maxdepth: 3

   contributing/development
   contributing/oven_drivers
   contributing/dynamic_extensions
   contributing/changelog
