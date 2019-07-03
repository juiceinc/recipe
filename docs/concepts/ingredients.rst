Ingredients
===========



----------
Formatters
----------

----------------
Building filters
----------------

Ingredient.build_filter


--------------------------------
Storing extra attributes in meta
--------------------------------


--------------------
Types of Ingredients
--------------------

List of ingredients

Dimension
~~~~~~~~~

Dimensions are groupings that exist in your data.

.. code-block:: python

    # A simple dimension
    self.shelf['state'] = Dimension(Census.state)

Adding an id
~~~~~~~~~~~~

Dimensions can support separate properties for ids and values. Consider a
table of employees with an ``employee_id`` and a ``full_name``. If you had
two employees with the same name you need to be able to distinguish between
them.

.. code-block:: python

    # Support an id and a label
    self.shelf['employee']: Dimension(Employee.full_name,
                                      id_expression=Employee.id)

The id is accessible as ``employee_id`` in each row and their full name is
available as ``employee``.

Using lookups
~~~~~~~~~~~~~

Lookup maps values in your data to descriptive names. The ``_id``
property of your dimension contains the original value.

.. code-block:: python

    # Convert M/F into Male/Female
    self.shelf['gender']: Dimension(Census.sex, lookup={'M': 'Male',
        'F': 'Female'}, lookup_default='Unknown')

If you use the gender dimension, there will be a ``gender_id`` in each row
that will be "M" or "F" and a ``gender`` in each row that will be "Male" or
"Female".

Metric
~~~~~~

DivideMetric
~~~~~~~~~~~~

WtdAvgMetric
~~~~~~~~~~~~

Filter
~~~~~~

Having
~~~~~~
