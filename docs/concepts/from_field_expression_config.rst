=======================================
Defining Shelves with Field Expressions
=======================================

Shelves can be created using dictionaries containing keys and ingredient definitions.
The shelf configuration can then be bound to a SQLAlchemy selectable.
The best way of doing this is to use the field expression syntax.
All the examples below use YAML to define a python dictionary.

Defining Shelves using Field Expressions
----------------------------------------

A simple example looks like this.

.. code:: YAML

  total_population:
    kind: Metric
    field: sum(pop2000)
  state:
    kind: Dimension
    field: state

See expression_examples_ for more Shelf examples.

.. _ingredients:

Defining Ingredients
--------------------

Ingredients are defined using fields_ defined in expression syntax.

Defining Metrics
~~~~~~~~~~~~~~~~

Metrics will always apply a default aggregation of 'sum' to any fields used.
The "Measure" can be used as a synonym of "Metric".

.. code::

    kind: Metric
    field: {field}

The field expression can use functions to perform aggregation. If no function
is provided then the field will be summed by default.

Math and functions on fields
............................

Fields can be added together and be wrapped in functions.

.. list-table:: Field function list
   :widths: 5 10 30
   :header-rows: 1

   * - Type
     - Function
     - Description
   * - field
     - {field}+{field}
     - Add two fields together
   * - field
     - {field}-{field}
     - Subtract a field from a field
   * - field
     - {field}*{field}
     - Multiply two fields
   * - field
     - {field}/{field}
     - Divide fields. 

       .. note:: This is a SQL safe division. Division by zero returns null.
   * - field
     - sum({field})
     - Sum up the values of {field}
   * - field
     - min({field})
     - Calculate the minumum value of {field}
   * - field
     - max({field})
     - Calculate the maximum value of {field}
   * - field
     - avg({field})
     - Calculate the average value of {field}
   * - field
     - median({field})
     - Calculate the median value of {field}. 
     
       .. note:: This aggregation is not available on all databases.
   * - field
     - percentile<n>({field})
     - Calculate the nth percentile value {field}
       
       <n> is one of 1,5,10,25,50,75,90,95,99

       .. note:: This aggregation is not available on all databases.
   * - field
     - count({field})
     - Count the number of values
   * - field
     - count_distinct({field})
     - Count the number of distinct values of {field}
   * - field
     - month({date_field})
     - Round to the nearest month for dates
   * - field
     - week({date_field})
     - Round to the nearest week for dates
   * - field
     - year({date_field})
     - Round to the nearest year for dates
   * - field
     - quarter({date_field})
     - Round to the nearest quarter for dates
   * - field
     - age({date_field})
     - Calculate current age in years for a date.

These functions and math can be combined. Division will be performed safely to ensure
that division by zero is not performed. Here's an example:

.. code::

  avg_profit_per_facility:
    kind: Metric
    field: sum(sales - expenses) / count(facilities)

Defining contant values and lists of values
...........................................

Values are numbers, strings or dates that can be used anywhere
a field is.

.. list-table:: How to define values
   :widths: 5 10 30
   :header-rows: 1

   * - Type
     - Examples
     - Description
   * - Strings
     - .. code:: 
     
          "STRING"
          "This is a string"

     - Strings are defined by double quoting.
   * - Numbers
     - .. code::

          1
          1.525
          1234890
          
     - Numbers can be integers or floating point values.
   * - Dates and times
     - .. code::

          "2016-02-20"
          "December 2019"
          "5 days ago"
          
     - Recipe uses `dateparser <https://dateparser.readthedocs.io/en/latest/#popular-formats>`_ to evaluate dates.

       Both absolute dates and relative dates can be defined.
   * - Lists of values
     - .. code::

         (1, 2, 3, 4, 5)
         (3.14, 2.72)
         ("apple", "peach")
     - Lists of values are comma separated within parentheses.

       All values should be the same type, but Recipe does not 
       validate this.

Values can be used in field math. Here are some examples:

.. code::

   avg_population:
     kind: Metric
     field: sum(population_in_2010 + population_in_2020) / 2.0
   tax_paid:
     kind: Metric
     field: sum(sales)*0.0725


Defining true and false conditions
...................................

Conditions can be used to calculate true or false values.

.. list-table:: How to calculate true or false expressions
   :widths: 5 10 30
   :header-rows: 1

   * - Type
     - Function
     - Description
   * - condition
     - {field} = {field}|{value}
     - Is a field equal to a field or a value
   * - condition
     - {field} != {field}|{value}
     - Is a field not equal to a field or a value
   * - condition
     - {field} > {field}|{value}
     - Is a field greater than a field or a value
   * - condition
     - {field} >= {field}|{value}
     - Is a field greater than or equal to a field or a value
   * - condition
     - {field} < {field}|{value}
     - Is a field less than a field or a value
   * - condition
     - {field} <= {field}|{value}
     - Is a field less than or equal to a field or a value
   * - condition
     - {field} IN ({list})
     - Is a field in a comma separate list of fields or values.
   * - condition
     - {field} NOT IN ({list})
     - Is a field not in a comma separate list of fields or values.
   * - condition
     - {field} BETWEEN {value} AND {value}
     - Is a field between two values.
   * - condition 
     - {condition} AND {condition}
     - Are both expressions true.
   * - condition 
     - {condition} OR {condition}
     - Is either condition true.
   
Using conditions and fields with the ``IF`` function
.....................................................

The ``IF`` function lets you combine conditions.

.. code::

  if({condition}, {field}, {else_field})

If the condition is true, use ``{{field}}`` otherwise use {{else_field}}.
More than one condition and field pair can can be provided.

.. code::

  if({condition1}, {field1}, {condition2}, {field2}, {else_field})

Let's look at an example. Here is how to sum up ``sales_dollars`` in the
last week.

.. code::

  sales_in_last_week:
    kind: Metric
    field: sum(if(sales_date>"7 days ago",sales_dollars,0.0))

Metrics must aggregate
......................

Metrics must define an aggregated field. If a Metric definition does not
include an aggregation function, it will be wrapped in a ``sum()``.

Defining Dimensions
~~~~~~~~~~~~~~~~~~~

Dimensions are simple to define but include a number of optional features. 

.. code::

    kind: Dimension
    field: {field}
    {role}_field: {field} (optional)
    buckets: A list of labeled conditions (optional)
    buckets_default_label: string (optional)
    quickselects: A list of labeled conditions (optional)

Defining simple dimensions
..........................

Dimensions can be use fields, expressions, conditions and even the ``IF``
function as long as they do not use aggregation functions. Here are some
examples.

.. code::

  hospital:
    kind: Dimension
    field: hospital_name
  student:
    kind: Dimension
    field: student_last_name
  student_full_name:
    kind: Dimension
    field: student_first_name + " " + student_last_name
  new_york_hospitals:
    kind: Dimension
    field: IF(state="New York",hospital_name,"Other")

Adding ``id`` and other roles to a Dimension
..........................................

Dimensions can be defined with extra fields. The prefix before ``_field``
is the field's role. The role will be **suffixed** to each value in the
recipe rows. Let's look at an example.

.. code::

  hospital:
    field: hospital_name
    id_field: hospital_id
    latitude_field: hospital_lat
    longitude_field: hospital_lng

Each result row will include

* ``hospital``
* ``hospital_id`` The field defined as ``id_field``
* ``hospital_latitude`` The field defined as ``latitude_field``
* ``hospital_longitude`` The field defined as ``longitude_field``

Defining buckets
................

Buckets let you group continuous values (like salaries or ages) into a dimension. 
Here's an example:

.. code:: YAML

  groups:
      kind: Dimension
      field: age
      buckets:
      - label: 'northeasterners'
        field: state
        in: ['Vermont', 'New Hampshire']
      - label: 'babies'
        lt: 2
      - label: 'children'
        lt: 13
      - label: 'teens'
        lt: 20
      buckets_default_label: 'oldsters'

The conditions are evaluated **in order**. **buckets_default_label** is used for any
values that didn't match any condition.

For convenience, conditions defined in buckets will use the field from the Dimension
unless a different field is defined in the condition. In the example above, the first
bucket uses ``field: state`` explicitly while all the other conditions use ``field: age``
from the Dimension.

If you use order_by a bucket dimension, the order will be the order in which the
buckets were defined.

Adding quickselects to a Dimension
..................................

quickselects are a way of associating conditions with a dimension.

.. code:: YAML

  region:
      kind: Dimension
      field: sales_region
  total_sales:
      kind: Metric
      field: sales_dollars
  date:
      kind: Dimension
      field: sales_date
      quickselects:
      - label: 'Last 90 days'
        between:
        - 90 days ago
        - tomorrow
      - label: 'Last 180 days'
        between:
        - 180 days ago
        - tomorrow

These conditions can then be accessed through ``Ingredient.build_filter``.
The ``AutomaticFilters`` extension is an easy way to use this.

.. code:: python

  recipe = Recipe(session=oven.Session(), extension_classes=[AutomaticFilters]). \
              .dimensions('region') \
              .metrics('total_sales') \
              .automatic_filters({
                'date__quickselect': 'Last 90 days'
              })

.. _expression_examples:

Examples
--------

A simple shelf with conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This shelf is basic.

.. code:: YAML

  teens:
      kind: Metric    
      field: sum(if(age 
      field:
          value: pop2000
          condition:
              field: age
              between: [13,19]
  state:
      kind: Dimension
      field: state

Using this shelf in a recipe.

.. code:: python

  recipe = Recipe(shelf=shelf, session=oven.Session())\
      .dimensions('state')\
      .metrics('teens')
  print(recipe.to_sql())
  print(recipe.dataset.csv)

The results look like:

.. code::

  SELECT census.state AS state,
        sum(CASE
                WHEN (census.age BETWEEN 13 AND 19) THEN census.pop2000
            END) AS teens
  FROM census
  GROUP BY census.state

  state,teens,state_id
  Alabama,451765,Alabama
  Alaska,71655,Alaska
  Arizona,516270,Arizona
  Arkansas,276069,Arkansas
  ...


Metrics referencing other metric definitions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following shelf has a Metric ``pct_teens`` that divides one previously defined Metric
``teens`` by another ``total_pop``.

.. code:: YAML

  teens:
      kind: Metric
      field:
          value: pop2000
          condition:
              field: age
              between: [13,19]
  total_pop:
      kind: Metric
      field: pop2000
  pct_teens:
      field: '@teens'
      divide_by: '@total_pop'
  state:
      kind: Dimension
      field: state

Using this shelf in a recipe.

.. code:: python

  recipe = Recipe(shelf=shelf, session=oven.Session())\
      .dimensions('state')\
      .metrics('pct_teens')
  print(recipe.to_sql())
  print(recipe.dataset.csv)

Here's the results. Note that recipe performs safe division.

.. code::

  SELECT census.state AS state,
        CAST(sum(CASE
                      WHEN (census.age BETWEEN 13 AND 19) THEN census.pop2000
                  END) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS pct_teens
  FROM census
  GROUP BY census.state

  state,pct_teens,state_id
  Alabama,0.10178190714599038,Alabama
  Alaska,0.11773975168751254,Alaska
  Arizona,0.10036487658951877,Arizona
  Arkansas,0.10330245760980436,Arkansas
  ...


Dimensions containing buckets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dimensions may be created by bucketing a field.

.. code:: YAML

  total_pop:
      kind: Metric
      field: pop2000
  age_buckets:
      kind: Dimension
      field: age
      buckets:
      - label: 'babies'
        lt: 2
      - label: 'children'
        lt: 13
      - label: 'teens'
        lt: 20
      buckets_default_label: 'oldsters'
  mixed_buckets:
      kind: Dimension
      field: age
      buckets:
      - label: 'northeasterners'
        in: ['Vermont', 'New Hampshire']
        field: state
      - label: 'babies'
        lt: 2
      - label: 'children'
        lt: 13
      - label: 'teens'
        lt: 20
      buckets_default_label: 'oldsters'

Using this shelf in a recipe.

.. code:: python

  recipe = Recipe(shelf=shelf, session=oven.Session())\
      .dimensions('mixed_buckets')\
      .metrics('total_pop')\
      .order_by('mixed_buckets')
  print(recipe.to_sql())
  print(recipe.dataset.csv)

Here's the results. Note this recipe orders by ``mixed_buckets``. The buckets are
ordered in the **order they are defined**.

.. code::

  SELECT CASE
            WHEN (census.state IN ('Vermont',
                                    'New Hampshire')) THEN 'northeasterners'
            WHEN (census.age < 2) THEN 'babies'
            WHEN (census.age < 13) THEN 'children'
            WHEN (census.age < 20) THEN 'teens'
            ELSE 'oldsters'
        END AS mixed_buckets,
        sum(census.pop2000) AS total_pop
  FROM census
  GROUP BY CASE
              WHEN (census.state IN ('Vermont',
                                      'New Hampshire')) THEN 'northeasterners'
              WHEN (census.age < 2) THEN 'babies'
              WHEN (census.age < 13) THEN 'children'
              WHEN (census.age < 20) THEN 'teens'
              ELSE 'oldsters'
          END
  ORDER BY CASE
              WHEN (census.state IN ('Vermont',
                                      'New Hampshire')) THEN 0
              WHEN (census.age < 2) THEN 1
              WHEN (census.age < 13) THEN 2
              WHEN (census.age < 20) THEN 3
              ELSE 9999
          END

  mixed_buckets,total_pop,mixed_buckets_id
  northeasterners,1848787,northeasterners
  babies,7613225,babies
  children,44267889,children
  teens,28041679,teens
  oldsters,199155741,oldsters

