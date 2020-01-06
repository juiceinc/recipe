===========
Ingredients
===========

Ingredients are the building block of recipe.

Ingredients can contain columns that are part of the ``SELECT`` portion of a query,
filters that are part of a ``WHERE`` clause of a query, group_bys that
contribute to a query's ``GROUP BY`` and havings which add ``HAVING`` limits
ot a query.

------------------------------
Creating ingredients in python
------------------------------

Ingredients can be created either in python or via configuration. To created
Ingredients in python, use one of the four convenience classes.

* **Metric**: Create an aggregated calculation using a column. This
  value appears only in the SELECT part of the SQL statement.
* **Dimension**: Create a non-aggregated value using a column. This
  value appears in the SELECT and GROUP BY parts of the SQL statement.
* **Filter**: Create a boolean expression. This value appears in the
  WHERE part of the SQL statement. Filters can be created automatically
  using the AutomaticFilters extension or by using a Dimension or Metric'sales
  ``build_filter`` method.
* **Having**: Create a boolean expression with an aggregated ColumnElement.
  This value appears in the HAVING part of the SQL statement.

Metrics and Dimensions are commonly reused in working Recipe code, while filters are
often created temporarily based on data.

Features of ingredients
-----------------------

Let's explore some capabilities.

Formatters
~~~~~~~~~~

Formatters are a list of python callables that take a single value. This
let you manipulate the results of an ingredient with python code. If you use
formatters, the original, unmodified value is available as ``{ingredient}_raw``.

.. code:: python

    shelf = Shelf({
        'state': Dimension(Census.state),
        'age': WtdAvgMetric(Census.age, Census.pop2000),
        'gender': Dimension(Census.gender),
        'population': Metric(func.sum(Census.pop2000), formatters=[
            lambda value: int(round(value, -6) / 1000000)
        ])
    })

    recipe = Recipe(shelf=shelf, session=oven.Session())\
        .dimensions('gender').metrics('population')

    for row in recipe.all():
        print('{} has {} people'.format(row.gender, row.population))
        print('\tThe original value is: {}'.format(row.population_raw))

The results look like

.. code::

    F has 144 million people
        The original value is: 143534804
    M has 137 million people
        The original value is: 137392517

Building filters
~~~~~~~~~~~~~~~~

Ingredient.build_filter


Storing extra attributes in meta
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Extra keyword arguments that get passed to ingredient initialization
get stored in the ``meta`` object. This can be used to extend the
capabilities of ingredients and add extra features.

.. code:: python

    d = Dimension(Census.age, icon='cog')
    print(d.meta.icon)
    >>> 'cog'

--------------------
Types of Ingredients
--------------------

List of ingredients

Dimension
---------

Dimensions are groupings that exist in your data. Dimension objects add
the column to the select statement and the group by of the SQL query.

.. code-block:: python

    # A simple dimension
    self.shelf['state'] = Dimension(Census.state)

Adding an id
~~~~~~~~~~~~

Dimensions can use separate columns for ids and values. Consider a
table of employees with an ``employee_id`` and a ``full_name``. If you had
two employees with the same name you need to be able to distinguish between
them.

.. code-block:: python

    # Support an id and a label
    self.shelf['employee']: Dimension(Employee.full_name,
                                      id_expression=Employee.id)

The id is accessible as ``employee_id`` in each row and their full name is
available as ``employee``.

If you build a filter using this dimension, it will filter against the id.

Adding an ordering
~~~~~~~~~~~~~~~~~~

If you want to order a dimension in a custom way, pass a keyword argument
``order_by_expression``. This code adds an order_by_expression that causes the
values to sort case insensitively.

.. code-block:: python

    from sqlalchemy import func

    # Support an id and a label
    self.shelf['employee']: Dimension(Employee.full_name,
                                      order_by_expression=func.lower(
                                        Employee.full_name
                                      ))

The order_by expression is accessible as ``employee_order_by`` in each row and
the full name is available as ``employee``. If the `employee` dimension is used in a
recipe, the recipe will **always** be ordered by ``func.lower(Employee.full_name)``.

Adding additional groupings
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both ``id_expression`` and ``order_by_expression`` are special cases of Dimension's
ability to be passed additional columns can be used for grouping. Any keyword argument
suffixed with ``_expression`` adds additional roles to this Dimension. The first
*required* expression supplies the dimension's value role. For instance,
you could create a dimension with an ``id``, a ``latitude`` and a ``longitude``.

For instance, the following

.. code-block:: python

    Dimension(Hospitals.name,
              latitude_expression=Hospitals.lat
              longitude_expression=Hospitals.lng,
              id='hospital')

would add columns named "hospital", "hospital_latitude", and
"hospital_longitude" to the recipes results. All three of these expressions
would be used as group bys.

Using lookups
~~~~~~~~~~~~~

You can use a lookup table to map values in your data to descriptive names. The ``_id``
property of your dimension contains the original value.

.. code-block:: python

    # Convert M/F into Male/Female
    self.shelf['gender']: Dimension(Census.sex, lookup={'M': 'Male',
        'F': 'Female'}, lookup_default='Unknown')

If you use the gender dimension, there will be a ``gender_id`` in each row
that will be "M" or "F" and a ``gender`` in each row that will be "Male" or
"Female".

.. code-block:: python

    shelf = Shelf({
        'state': Dimension(Census.state),
        'gender_desc': Dimension(Census.gender, lookup={'M': 'Male',
            'F': 'Female'}, lookup_default='Unknown'),
        'age': WtdAvgMetric(Census.age, Census.pop2000),
        'population': Metric(func.sum(Census.pop2000))
    })

    recipe = Recipe(shelf=shelf, session=oven.Session())\
        .dimensions('gender_desc').metrics('population')
    print(recipe.to_sql())
    print(recipe.dataset.csv)

Lookups inject a formatter in the first position. Because a formatter
is used, recipe creates a ``gender_desc_raw`` on the response that
contains the unformatted value then uses the lookup to create the ``gender_desc``
property. All dimensions also generate an ``{ingredient}_id`` property.

Here is the query and the results.

.. code-block::

    SELECT census.gender AS gender_desc_raw,
        sum(census.pop2000) AS population
    FROM census
    GROUP BY census.gender

    gender_desc_raw,population,gender_desc,gender_desc_id
    F,143534804,Female,F
    M,137392517,Male,M


Metric
------

Metrics are aggregations performed on your data. Here's an example
of a few Metrics.

.. code-block:: python

    shelf = Shelf({
        'total_population': Metric(func.sum(Census.pop2000)),
        'min_population': Metric(func.min(Census.pop2000)),
        'max_population': Metric(func.max(Census.pop2000))
    })
    recipe = Recipe(shelf=shelf, session=oven.Session())\
        .metrics('total_population', 'min_population', 'max_population')
    print(recipe.to_sql())
    print(recipe.dataset.csv)

The results of this recipe are:

.. code::

    SELECT max(census.pop2000) AS max_population,
        min(census.pop2000) AS min_population,
        sum(census.pop2000) AS total_population
    FROM census

    max_population,min_population,total_population
    294583,217,280927321


DivideMetric
------------

Division in SQL introduces the possibility of division by zero. DivideMetric
guards against division by zero while giving you a quick way to divide
one calculation by another.

.. code:: python

    shelf = Shelf({
        'state': Dimension(Census.state),
        'popgrowth': DivideMetric(func.sum(Census.pop2008-Census.pop2000), func.sum(Census.pop2000)),
    })
    recipe = Recipe(shelf=shelf, session=oven.Session())\
        .dimensions('state').metrics('popgrowth')

This creates results like:

.. code::

    SELECT census.state AS state,
        CAST(sum(census.pop2008 - census.pop2000) AS FLOAT) /
          (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS popgrowth
    FROM census
    GROUP BY census.state

    state,popgrowth,state_id
    Alabama,0.04749469366071285,Alabama
    Alaska,0.09194726152996757,Alaska
    Arizona,0.2598860676785905,Arizona
    Arkansas,0.06585681816651036,Arkansas
    California,0.0821639328251409,California
    Colorado,0.14231283526592364,Colorado
    ...

The denominator has a tiny value added to it to prevent division by zero.

WtdAvgMetric
------------

``WtdAvgMetric`` generates a weighted average of a number using a weighting.

.. warning::

    ``WtdAvgMetric`` takes two ColumnElements as arguments. The first is the value
    and the second is the weighting. Unlike other Metrics, these are **not aggregated**.

Here's an example.

.. code:: python

    shelf = Shelf({
        'state': Dimension(Census.state),
        'avgage': WtdAvgMetric(Census.age, Census.pop2000),
    })
    recipe = Recipe(shelf=shelf, session=oven.Session())\
        .dimensions('state').metrics('avgage')

    print(recipe.to_sql())
    print(recipe.dataset.csv)

This generates results that look like this:

.. code::

    SELECT census.state AS state,
        CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS avgage
    FROM census
    GROUP BY census.state

    state,avgage,state_id
    Alabama,36.27787892421841,Alabama
    Alaska,31.947384766048568,Alaska
    Arizona,35.37065466080318,Arizona
    Arkansas,36.63745110262778,Arkansas
    California,34.17872597484759,California
    ...

Note: WtdAvgMetric uses safe division from ``DivideMetric``.

Filter
------

Filter objects add a condition to the where clause of your SQL query.
Filter objects can be added to a Shelf.

.. code:: python

    shelf = Shelf({
        'state': Dimension(Census.state),
        'population': Metric(func.sum(Census.pop2000)),
        'teens': Filter(Census.age.between(13,19)),
    })
    recipe = Recipe(shelf=shelf, session=oven.Session())\
        .dimensions('state')\
        .metrics('population')\
        .filters('teens')
    print(recipe.to_sql())
    print(recipe.dataset.csv)

This results in output like:

.. code::

    SELECT census.state AS state,
        sum(census.pop2000) AS population
    FROM census
    WHERE census.age BETWEEN 13 AND 19
    GROUP BY census.state

    state,population,state_id
    Alabama,451765,Alabama
    Alaska,71655,Alaska
    Arizona,516270,Arizona

Different ways of generating Filters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Recipe has several ways of filtering recipes.

* **Filter objects can be added to the shelf**. They can be added to the
  recipe by name from a shelf. This is best when
  you have a filter that you want to use in many place.

  .. code:: python

        shelf = Shelf({
            'age': Dimension(Census.age),
            'state': Dimension(Census.state),
            'population': Metric(func.sum(Census.pop2000)),
            'teens': Filter(Census.age.between(13,19)),
        })
        ...
        recipe = recipe.filters('teens')

* **Filter objects can be created dynamically** and added to the recipe. This is
  best if the filtering needs to change dynamically.

  .. code:: python

        recipe = recipe.filters(Filter(Census.age.between(13,19))

* **Ingredient.build_filter**  can be used to build filters that refer
  to the ingredient's column.

  .. code:: python

    age_filter = shelf['age'].build_filter([13,19], 'between')
    recipe = recipe.filters(age_filter)

  This is best when you want to reuse a column definition defined in
  an ingredient.
* **AutomaticFilters**: The AutomaticFilters extension adds filtering
  syntax directly to recipe.

  .. code:: python

    recipe = recipe.automatic_filters({
      'age__between': [13,19]
    })

  This is best when you want to add many filters consistently.
  AutomaticFilters uses ``Ingredient.build_filter`` behind the scenes.

Having
------

Having objects are binary expressions with an aggregated column value.
One easy way to generate ``Having`` objects is to ``build_filter`` using
a ``Metric``.

.. code:: python

    shelf = Shelf({
        'age': Dimension(Census.age),
        'avgage': WtdAvgMetric(Census.age, Census.pop2000),
        'state': Dimension(Census.state),
        'population': Metric(func.sum(Census.pop2000)),
    })
    # Find states with a population greater than 15 million
    big_states = shelf['population'].build_filter(15000000, operator='gt')
    recipe = Recipe(shelf=shelf, session=oven.Session())\
        .dimensions('state')\
        .metrics('population')\
        .order_by('-population')\
        .filters(big_states)

    print(recipe.to_sql())
    print(recipe.dataset.csv)

This generates the following results.

.. code::

    SELECT census.state AS state,
        sum(census.pop2000) AS population
    FROM census
    GROUP BY census.state
    HAVING sum(census.pop2000) > 15000000
    ORDER BY sum(census.pop2000) DESC

    state,population,state_id
    California,33829442,California
    Texas,20830810,Texas
    New York,18978668,New York
    Florida,15976093,Florida
