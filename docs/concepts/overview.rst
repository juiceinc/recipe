.. _concepts_overview:

Overview of Recipe Concepts
===========================

**Ingredients** are reusable fragments of SQL defined in SQLAlchemy. Ingredients
can contribute to a SQL query's select, group by, where clause or having clause.
Recipe defines **Metric**, **Dimension**, **Filter**, and **Having**
classes which support common query patterns.

A **Shelf** is a container for holding named ingredients. 
Shelves can be defined with python code or via configuration.
Shelves defined with configuration can be bound to a SQLAlchemy selectable.

.. note::

    By convention, all the ingredients on a Shelf should reference the same SQLAlchemy selectable.

A **Recipe** uses a **Shelf**. The Recipe picks dimensions, metrics, filters,
and havings from the shelf. Dimensions and metrics can also be used to order results.
While the **Recipe** can refer to items in the shelf by name, you can also supply
raw Ingredient objects. Recipe uses a builder pattern to allow a recipe object to be 
modified.

A Recipe generates and runs a SQL query using SQLAlchemy. The query uses an **Oven**
an abstraction on top of a SQLAlchemy connection. The query results are "enchanted"
which adds additional properties to each result row. This allows ingredients to 
format or transform values with python code.

Recipe results can optionally be cached with the recipe_caching support library.

Extensions
----------

Extensions add to Recipe to change how SQL queries get built.

Recipe includes the following built-in extensions.

* **AutomaticFilter**: Supports a configuration syntax for applying filters.
* **SummarizeOver**: Supports summarizing over a dimension
* **BlendRecipe**: Allows data from different tables to be combined
* **CompareRecipe**: Allows a secondary recipe against the same table to be combined.
* **Anonymize**: Allows result data to be anonymized.
