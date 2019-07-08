===================================
Defining Shelves From Configuration
===================================

Shelves are defined as dictionaries containing keys and ingredient. 
All the examples below use YAML.

Defining Ingredients
--------------------

Ingredients are defined recursively using fields (which may contain conditions) 
and conditions (which include fields).

Field syntax
------------

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

Dictionary field definitions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

   * - ref 
     - optional
     - string

       **For internal use**

       Replace this field with the field defined in
       the specified key in the shelf.

   * - _use_raw_value 
     - optional
     - boolean

       **For internal use**

       Don't evaluate value as a column, treat
       it as a constant in the SQL expression.


String field definitions
~~~~~~~~~~~~~~~~~~~~~~~~

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


Operator syntax
---------------

A list of operators lets you perform math with fields.

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


Condition syntax
----------------

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
    - condition:
        field: state 
        like: 'C%'
    - condition:
        field: state 
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

Quickfilters are a feature of Dimension that are defined with a list
of labeled conditions.

Examples
--------




   