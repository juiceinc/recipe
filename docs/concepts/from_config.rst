===================================
Defining Shelves from configuration
===================================

Shelves are defined as dictionaries containing keys and ingredient.
All the examples below use YAML.

Defining Shelves
----------------

Shelves are defined in configuration as dictionaries with keys and values that
are Ingredient configuration definitions. A simple example looks like this.

.. code:: YAML

  total_population:
    kind: Metric
    field: pop2000
  state:
    kind: Dimension
    field: state

See examples_ for more Shelf examples.

.. _ingredients:

Defining Ingredients
--------------------

Ingredients are defined using fields_ (which may contain conditions_). Those conditions_
may reference more fields_ in turn and so forth.

Metric
~~~~~~

Metrics will always apply a default aggregation of 'sum' to any fields used.

.. code::

    kind: Metric
    field: {field}
    divide_by: {field} (optional)

``divide_by`` is an optional denominator that ``field`` will be divided by safely.

Dimension
~~~~~~~~~

Metrics will always apply a default aggregation of 'sum' to their field.

.. code::

    kind: Dimension
    field: {field}
    {role}_field: {field} (optional)
    buckets: A list of labeled conditions (optional)
    buckets_default_label: string (optional)
    quickselects: A list of labeled conditions (optional)

Adding `id` and other roles to Dimension
........................................

Dimensions can be defined with extra fields. The prefix before ``_field``
is the field's role. The role will be suffixed to each value in the
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

Buckets let you group continuous values (like salaries or ages). Here's
an example:

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

.. _fields:

Defining Fields
---------------

Fields can be defined with a short string syntax or a dictionary syntax.
The string syntax always is normalized into the dictionary syntax.

.. code::

    field:
        value: '{column reference}'
        aggregation: '{aggregation (optional)}'
        operators: {list of operators}
        as: {optional type to coerce into}
        default: {default value, optional}

    or

    field: '{string field definition}'
    This may include field references that look like
    @{ingredient name from the shelf}.

Defining Fields with Dicts
~~~~~~~~~~~~~~~~~~~~~~~~~~

Dictionaries provide access to all options when defining a
field.

.. list-table:: dictionary field options
   :widths: 10 5 30
   :header-rows: 1

   * - Key
     - Required
     - Description
   * - value
     - required
     - string

       What column to use.
   * - aggregation
     - optional
     - string

       (default is 'sum' for Metric and 'none' for Dimension)

       What aggregation to use, if any. Possible aggregations are:

       - 'sum'
       - 'min'
       - 'max'
       - 'avg'
       - 'count'
       - 'count_distinct'
       - 'month' (round to the nearest month for dates)
       - 'week' (round to the nearest week for dates)
       - 'year' (round to the nearest year for dates)
       - 'quarter' (round to the nearest quarter for dates)
       - 'age' (calculate age based on a date and the current date)
       - 'none' (perform no aggregation)
       - 'median' (calculate the median value, note: this aggregation is not available
         on all databases).
       - 'percentile[1,5,10,25,50,75,90,95,99]' (calculate the nth percentile value
         where higher values correspond to higher percentiles, note: this aggregation
         is not available on all databases).

   * - condition
     - optional
     - A ``condition``

       Condition will limit what rows of data are aggregated for a field.

   * - operators
     - optional
     - A list of ``operator``

       Operators are fields combined with a math operator to the base field.

   * - default
     - optional
     - An integer, string, float, or boolean value (optional)

       A value to use if the column is NULL.

.. warning:: The following two fields are for internal use.

.. list-table:: internal dictionary field options
   :widths: 10 5 30
   :header-rows: 1

   * - Key
     - Required
     - Description

   * - ref
     - optional
     - string

       Replace this field with the field defined in
       the specified key in the shelf.

   * - _use_raw_value
     - optional
     - boolean

       Don't evaluate value as a column, treat
       it as a constant in the SQL expression.


Defining Fields with Strings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fields can be defined using strings. When using
strings, words are treated as column references. If the
words are prefixed with an '@' (like @sales), the field of the ingredient
named sales in the shelf will be injected.

Aggregations can be called like functions to apply that aggregation
to a column.

.. list-table:: string field examples
   :widths: 10 20
   :header-rows: 1

   * - Field string
     - Description

   * - revenue - expenses
     - The sum of column revenue minus the sum of column expenses.

       .. code::

         field: revenue - expenses

         # is the same as

         field:
           value: revenue
           aggregation: sum  # this may be omitted because 'sum'
                             # is the default aggregation for Metrics
           operators:
           - operator: '-'
             field:
               value: expenses
               aggregation: sum

   * - @sales / @student_count
     - Find the field definition of the field named 'sales' in the shelf.

       Divide it by the field definition of the field named 'student_count'.

   * - count_distinct(student_id)
     - Count the distinct values of column student_id.

       .. code::

         field: count_distinct(student_id)

         # is the same as

         field:
            value: student_id
            aggregation: count_distinct

.. _operators:

Defining Field Operators
------------------------

Operators lets you perform math with fields.

.. list-table:: operator options
   :widths: 10 5 30
   :header-rows: 1

   * - Key
     - Required
     - Description
   * - operator
     - required
     - string

       One of '+', '-', '*', '/'

   * - field
     - required
     - A field definition (either a string or a dictionary)

For instance, operators can be used like this:

.. code:: YAML

  # profit - taxes - interest
  field:
    value: profit
    operators:
    - operator: '-'
      field: taxes
    - operator: '-'
      field: interest

.. _conditions:

Defining Conditions
-------------------

Conditions can include a field and operator or a list of
conditions and-ed or or-ed together.

.. code::

    field: {field definition}
    label: string (an optional string label)
    {operator}: {value} or {list of values}

    or

    or:     # a list of conditions
    - {condition1}
    - {condition2}
    ...
    - {conditionN}

    or

    and:    # a list of conditions
    - {condition1}
    - {condition2}
    ...
    - {conditionN}

    or

    a condition reference @{ingredient name from the shelf}.


Conditions consist of a field and **exactly one** operator.

.. list-table:: condition options
   :widths: 10 5 30
   :header-rows: 1

   * - Condition
     - Value is...
     - Description
   * - gt
     - A string, int, or float.
     - Find values that are greater than the value

       For example:

       .. code::

         # Sales dollars are greater than 100.
         condition:
           field: sales_dollars
           gt: 100

   * - gte (or ge)
     - A string, int, or float.
     - Find values that are greater than or equal to the value

   * - lt
     - A string, int, or float.
     - Find values that are less than the value

   * - lte (or le)
     - A string, int, or float.
     - Find values that are less than or equal to the value

   * - eq
     - A string, int, or float.
     - Find values that are equal to the value

   * - ne
     - A string, int, or float.
     - Find values that are not equal to the value

   * - like
     - A string
     - Find values that match the SQL LIKE expression

       For example:

       .. code::

         # States that start with the capital letter C
         condition:
           field: state
           like: 'C%'

   * - ilike
     - A string
     - Find values that match the SQL ILIKE (case insensitive like) expression.

   * - between
     - A list of **two** values
     - Find values that are between the two values.

   * - in
     - A list of values
     - Find values that are in the list of values

   * - notin
     - A list of values
     - Find values that are not in the list of values

ands and ors in conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~

Conditions can ``and`` and ``or`` a list of conditions together.

Here's an example:

.. code:: YAML

  # Find states that start with 'C' and end with 'a'
  # Note the conditions in the list don't have to
  # use the same field.
  condition:
    and:
    - field: state
      like: 'C%'
    - field: state
      like: '%a'

Date conditions
~~~~~~~~~~~~~~~

If the ``field`` is a date or datetime, absolute and relative dates
can be defined in values using string syntax. Recipe uses the
`Dateparser <https://dateparser.readthedocs.io/en/latest/>`_ library.

Here's an example.

.. code:: YAML

  # Find sales that occured within the last 90 days.
  condition:
    field: sales_date
    between:
    - '90 days ago'
    - 'tomorrow'

Labeled conditions
~~~~~~~~~~~~~~~~~~

Conditions may optionally be labeled by adding a label property.

quickselects are a feature of Dimension that are defined with a list
of labeled conditions.

.. _examples:

Examples
--------

A simple shelf with conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This shelf is basic.

.. code:: YAML

  teens:
      kind: Metric
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

