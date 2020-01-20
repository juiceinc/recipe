====================================
Defining Shelves using configuration
====================================

Configuration lets you define fields and conditions using natural, SQL-like
language.

.. note::

    An older version of defining shelves from config can be found at :ref:`shelves_from_config_v1`.


Defining Shelves
----------------

Shelves are defined in configuration as dictionaries with keys and values that
are Ingredient configuration definitions. A simple example (configured in yaml)
looks like this.

.. code-block::

  _version: "2"
  total_population:
    kind: Metric
    field: pop2000
  state:
    kind: Dimension
    field: state

The `_version: "2"` key is necessary to trigger the new shelf behavior.

See examples_ for more Shelf examples.


Fields
------

The equivalent of the SQLAlchemy ``expression`` used in Ingredients defined in Python
is ``field``. This is a string that will be parsed into a SQLAlchemy expression
using a selectable (a table, recipe or subquery used to fetch data).

Fields are defined using strings.

When used in a ``Metric``, the field may contain
aggregations. If not aggregation is provided, the entire field string will be wrapped
in a ``sum()``.

When used in a ``Dimension``, fields must not contain aggregations. An BadIngredient
exception will be raised if you define a field this way.

Here are some examples of non-aggregated fields that you could use in a ``Dimension``.

.. list-table:: Sample non-aggregated fields in Dimensions
   :widths: 20 20
   :header-rows: 1

   * - Description
     - Definition
   * - Use the column student_name in your selectable.
     - .. code-block::

         student:
             kind: Dimension
             field: student_name

   * - Use the column student_name in your selectable as the value for the field
       and uses the student_id column as the id.
     - .. code-block::

         student:
             kind: Dimension
             field: student_name
             id_field: student_id

   * - Concatenate the student first and last names as the value for the field
       and uses the student_id column as the id.
     - .. code-block::

         student:
             kind: Dimension
             field: 'student_first_name + " " + student_last_name'
             id_field: student_id

Here's an example of some aggregated fields that you could use in metrics


.. list-table:: Sample aggregated fields in Metrics
   :widths: 20 20
   :header-rows: 1

   * - Description
     - Definition
   * - Count the number of rows in your data
     - .. code-block::

         count:
             kind: Metric
             field: count(*)

   * - Count the number of distinct student names.
     - .. code-block::

         student_cnt:
             kind: Metric
             field: count_distinct(student_name)

   * - Sum the value in the sales column in your selectable.
     - .. code-block::

         total_sales:
             kind: Metric
             field: sum(sales)

   * - Sum the value in the sales column and subtract the sum of expenses in your
       selectable.
     - .. code-block::

         profit:
             kind: Metric
             field: sum(sales) - sum(expenses)


Aggregations are written function-style like ``sum(sales)``. The following aggregations are available:

   - sum(<field>)
   - min(<field>)
   - max(<field>)
   - avg(<field>)
   - count(<field>)
   - count_distinct(<field>)
   - month(<field>) (round to the nearest month for dates)
   - week(<field>) (round to the nearest week for dates)
   - year(<field>) (round to the nearest year for dates)
   - quarter(<field>) (round to the nearest quarter for dates)
   - age(<field>) (calculate age based on a date and the current date)
   - none(<field>) (perform no aggregation)
   - median(<field>) (calculate the median value, note: this aggregation is not available
     on all databases).
   - percentile[1,5,10,25,50,75,90,95,99](<field>) (calculate the nth percentile value
     where higher values correspond to higher percentiles, note: this aggregation
     is not available on all databases).


Defining if-then logic in fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fields can contain an ``if()`` function which contains one or more conditions. It
looks like this.

.. code::

  if(<condition>, <field>, [<condition>, <field>,] [<else_field>])

Here's some examples:

.. list-table:: Sample ifs
   :widths: 20 20
   :header-rows: 1

   * - Description
     - Definition
   * - Count alerts if a certain status_code is matched
     - .. code-block::

         alert_cnt:
           kind: Metric
           field: count_distinct(if(status_code=5, alert_id))

   * - Discount sales based on codes, but sum without a discount when the right code
       doesn't exist.
     - .. code-block::

         discount_total:
             kind: Metric
             field: sum(if(discount_code=1,sales*0.9,discount_code=2,sales*0.8,sales)

   * - Discount sales based on codes, but sum without a discount when the right code
       doesn't exist.
     - .. code-block::

         discount_total:
             kind: Dimension
             field: if(last_name,first_name + " " + last_name,first_name)

Conditions
----------

Conditions are expressions that evaluate as true or false.

.. list-table:: Conditions
   :widths: 5 20
   :header-rows: 1

   * - Condition
     - Description
   * - >
     - Find values that are greater than the value

       For example:

       .. code::

         # Sales dollars are greater than 100.
         condition: sales_dollars>100

       or

       .. code::

         # Sales dollars are greater than 100.
         condition: last_name>"C"

   * - >=
     - Find values that are greater than or equal to the value

   * - <
     - Find values that are less than the value

   * - <=
     - Find values that are less than or equal to the value

   * - =
     - Find values that are equal to the value

   * - !=
     - Find values that are not equal to the value

   * - between <value> and <value>
     - Find values that are between the two values.

       .. code::

         # Sales dollars are between than 100 and 200.
         condition: sales between 100 and 200

       or

       .. code::

         # Sales dollars are between than 100 and 200.
         condition: 'sales_date between "2 weeks ago" and "tomorrow"'

   * - in (list of <values>)
     - Find values that are in the list of values

       .. code::

         # New England states in the USA
         condition: state_abbreviation in ("VT", "NH", "ME", "MA", "CT")

   * - not in
     - Find values that are not in the list of values

       .. code::

         condition: sales_code not in (1,5,7,9)


Using ands and ors in conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Conditions can ``and`` and ``or`` multiple conditions together.

Here's an example:

.. code:: YAML

  # Find sales between 100 and 1000
  condition: sales_dollars > 100 and sales_dollars < 1000

You can also use parentheses to clearly express groupings.

.. code:: YAML

  # Find sales meeting multiple conditions
  condition: (sales_dollars > 100 or sales_date > "1 month ago") and region = "North"


Date conditions
~~~~~~~~~~~~~~~

If the ``field`` is a date or datetime, absolute and relative dates
can be defined in values using string syntax. Recipe uses the
`Dateparser <https://dateparser.readthedocs.io/en/latest/>`_ library.

Here's an example.

.. code:: YAML

  # Find sales that occured within the last 90 days.
  condition: 'sales_date between "90 days ago" and "tomorrow"'

.. _partial_conditions:

Partial conditions
~~~~~~~~~~~~~~~~~~

While most conditions have to contain a field, condition and value (like
``sales_dollars>1000``), in some contexts you can define a partial condition that
contains just the condition and value (``>1000``). The field will be automatically
prefixed to each partial condition.


.. _ingredients:

Extra features
--------------

Metric fields always apply an aggregation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Metrics will always apply a default aggregation of 'sum' to any fields used.

.. code::

    sales:
      kind: Metric
      field: sales_dollars

is the same as

.. code::

    sales:
      kind: Metric
      field: sum(sales_dollars)


Defining extra roles in dimensions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dimensions can contain extra groupings (see :ref:`dimension_roles`). In configuration
you can define extra roles by creating extra keys that end with ``_field``. For instance:

.. code::

    student:
      kind: Dimension
      field: 'student_first_name + " " + student_last_name'
      id_field: student_id

Defining bucket dimensions
~~~~~~~~~~~~~~~~~~~~~~~~~~

A common need is to group values and treat those groupings as a dimension.
For instance, you could group sales as small, medium or large.

Dimension allows you to define a list of labeled conditions that you can use to
do exactly this. Let's look at an example then break it down.

.. code::

    kind: Dimension
    field: sales_dollars
    buckets:
    - label: Small
      condition: <1000
    - label: Medium
      condition: <20000
    - label: Large
      condition: >=20000
    buckets_default_label: Unknown

These conditions can be full or partial conditions (:ref:`partial_conditions`). In this
example the ``sales_dollars`` would be prefixed to all conditions, making it
identical to this.


.. code::

    kind: Dimension
    field: sales_dollars
    buckets:
    - label: Small
      condition: sales_dollars<1000
    - label: Medium
      condition: sales_dollars<20000
    - label: Large
      condition: sales_dollars>=20000
    buckets_default_label: Unknown

The ``buckets_default_label`` is applied when none of the bucket conditions match
(for instance, if the sales_dollars was NULL in this example). A bucket Dimension will
include an order_by that orders results in the order that the buckets were defined.

.. note::

    Buckets create a ``if()`` function to create their groupings

    In our sample bucket code, we could accomplish the same thing with these
    fields (broken into separate lines for clarity).

    .. code::

        kind: Dimension
        field: 'if(sales_dollars<1000,"Small",
                  sales_dollars<20000,"Medium",
                  sales_dollars>=20000,"Large","Unknown")'
        order_by_field: 'if(sales_dollars<1000,1,
                          sales_dollars<20000,2,
                          sales_dollars>=20000,3,9999)'


Adding quickselects to a Dimension
..................................

quickselects are a way of associating named conditions with a Dimension. Like buckets
quickselects use partial conditions.

.. code-block::

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
      - name: 'Last 90 days'
        condition: 'between "90 days ago" and "tomorrow"'
      - name: 'Last 180 days'
        condition: 'between "180 days ago" and "tomorrow"'

These conditions can then be accessed through ``Ingredient.build_filter``.
The ``AutomaticFilters`` extension is an easy way to use this.

.. code:: python

  recipe = Recipe(session=oven.Session(), extension_classes=[AutomaticFilters]). \
              .dimensions('region') \
              .metrics('total_sales') \
              .automatic_filters({
                'date__quickselect': 'Last 90 days'
              })

.. _examples:

Examples
--------

A simple shelf with conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This shelf is basic.

.. code:: YAML

  _version: "2"
  teens:
      kind: Metric
      field: if(age between 13 and 19,pop2000)
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

