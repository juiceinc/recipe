.. _quickstart:

===============
Getting Started
===============
 
.. module:: recipe

This page gives a good introduction in how to get started with Recipe. This
assumes you already have Recipe installed. If you do not, head over to
:ref:`Installing Recipe <install>`.

First, make sure that:

* Recipe is :ref:`installed <install>`
* Recipe is :ref:`up-to-date <updates>`

Let's gets started with some simple use cases and examples.

------------------
Creating a Shelf
------------------

A :class:`Shelf <recipe.Shelf>` is a place to store SQL fragments. In recipe
these are called :class:`Ingredients <recipe.Ingredient>`. 

Ingredients can contain columns that should be part of the ``SELECT`` portion of a query,
filters that are part of a ``WHERE`` clause of a query, group_bys that
contribute to a query's ``GROUP BY`` and havings which add ``HAVING`` limits
to a query.

You won't have to construct an Ingredient
with all these parts directly because Recipe contains convenience classes
that help you build the most common SQL fragments. The two most common
Ingredient subclasses are :class:`Dimension <recipe.Dimension>` which provides
both a column and a grouping on that column and
:class:`Metric <recipe.Metric>` which provides a column aggregation.

Shelf acts like a dictionary. The keys are strings and the
values are Ingredients. The keys are a shortcut name for the
ingredient. Here's an example.

.. code:: python

    from recipe import *

    # Define a database connection
    oven = get_oven('sqlite://')
    Base = declarative_base(bind=oven.engine)

    # Define a SQLAlchemy mapping
    class Census(Base):
        state = Column('state', String(), primary_key=True)
        sex = Column('sex', String())
        age = Column('age', Integer())
        pop2000 = Column('pop2000', Integer())
        pop2008 = Column('pop2008', Integer())

        __tablename__ = 'census'
        __table_args__ = {'extend_existing': True}

    # Use that mapping to define a shelf.
    shelf = Shelf({
        'state': Dimension(Census.state),
        'age': WtdAvgMetric(Census.age, Census.pop2000),
        'population': Metric(func.sum(Census.pop2000))
    })

This is a shelf with two metrics (a weighted average of age, and the sum of
population) and a dimension which lets you group on US State names.

---------------------------------
Using the Shelf to build a Recipe
---------------------------------

Now that you have the shelf, you can build a :class:`Recipe <recipe.Recipe>`.

.. code:: python

    r = Recipe(shelf=shelf, session=oven.Session())\
        .dimensions('state')\
        .metrics('age')\
        .order_by('-age')

    print(r.dataset.csv)

This results in 

.. code:: 

    state,age,state_id
    Florida,39.08283934000634,Florida
    West Virginia,38.555058651148165,West Virginia
    Maine,38.10118393261269,Maine
    Pennsylvania,38.03856695544053,Pennsylvania
    Rhode Island,37.20343773873182,Rhode Island
    Connecticut,37.19867141455273,Connecticut
    ...

Note that a recipe contains data from a single table.`


------------------------------------------------
Defining Shelves and Recipes Using Configuration
------------------------------------------------

Recipes and shelves can be defined using plain ole' python objects.
In the following example we'll use YAML. For instance, we can define
the shelf using this yaml config. 

.. code:: YAML

    state:
        kind: Dimension
        field: state
    age:
        kind: WtdAvgMetric
        field: age
        weight: pop2000
    population:
        kind: Metric
        field: pop2000

We can load this config by parsing it against any **selectable**, which
can be a SQLAlchemy mapping, a SQLAlchemy select, or another Recipe.

.. code:: python

    shelf_yaml = yaml.load('shelf.yaml')
    s = Shelf.from_config(shelf_yaml, Census)

We can also define a Recipe with Configuration

.. code:: YAML

    metrics:
    - age
    - population
    dimensions:
    - state
    order_by:
    - '-age'

If we load that we get a Recipe

.. code:: python

    recipe_yaml = yaml.load('shelf.yaml')
    recipe = Recipe.from_config(s, recipe_yaml, session=oven.Session())
    print(recipe.dataset.csv)

This results in a list of the oldest US states and their populations:

.. code::

    state,age,population,state_id
    Florida,39.08283934000634,15976093,Florida
    West Virginia,38.555058651148165,1805847,West Virginia
    Maine,38.10118393261269,1271694,Maine
    Pennsylvania,38.03856695544053,12276157,Pennsylvania
    Rhode Island,37.20343773873182,1047200,Rhode Island
    Connecticut,37.19867141455273,3403620,Connecticut
    ...


-------------------------------
Adding Features with Extensions
-------------------------------

Using extensions, you can add features to Recipe. Here are a few
interesting thing you can do. This example mixes in two extensions. 

**AutomaticFilters** defines filters (where clauses) using configuration. 
In this case were are filtering to states that start with the letter C.

**CompareRecipe** mixes in results from another recipe. In this case,
we are using this comparison recipe to calculate an average age across 
all states.

.. code:: python

    recipe_yaml = yaml.load(r)
    recipe = Recipe.from_config(s, recipe_yaml, session=oven.Session(), 
        extension_classes=(AutomaticFilters, CompareRecipe))\
        .automatic_filters({'state__like': 'C%'})\
        .compare(Recipe(shelf=s, session=oven.Session()).metrics('age')) 
    print(recipe.to_sql())   
    print()
    print(recipe.dataset.csv)

The output looks like this

.. code:: SQL

    SELECT census.state AS state,
        CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS age,
        sum(census.pop2000) AS population,
        avg(anon_1.age) AS age_compare
    FROM census
    LEFT OUTER JOIN
    (SELECT CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS age
    FROM census) AS anon_1 ON 1=1
    WHERE census.state LIKE 'C%'
    GROUP BY census.state
    ORDER BY CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) DESC

    state,age,population,age_compare,state_id
    Connecticut,37.19867141455273,3403620,35.789568740450036,Connecticut
    Colorado,34.5386073584527,4300877,35.789568740450036,Colorado
    California,34.17872597484759,33829442,35.789568740450036,California


Now, go check out the :ref:`API Documentation <api>` or look at an :ref:`concepts_overview`.
 