.. _quickstart:

==========
Quickstart
==========


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
ot a query.

It's a safe bet that you won't have to construct an Ingredient
with all these parts directly because Recipe contains convenience classes
that help you build the most common SQL fragments. The two most common
Ingredient subclasses are :class:`Dimension <recipe.Dimension>` which supply
both a column and a grouping on that column and
:class:`Metric <recipe.Metric>` which supply a column aggregation.

Shelf acts like a dictionary. The keys are strings and the
values are Ingredients. The keys are a shortcut name for the
ingredient. Here's an example.

::

    from recipe import *

    shelf = Shelf({
        'state', Dimension(Census.state),
        'age': WtdAvgMetric(Census.age, Census.pop2000),
        'population': Metric(func.sum(Census.pop2000))
    })

This is a shelf with two metrics (a weighted average of age, and the sum of
population) and a dimension which lets you group on US State names.


---------------------------------
Using the Shelf to build a Recipe
---------------------------------

Now that you have the shelf, you can build a recipe

Quick example of a recipe

Basic parts of a recipe

dimension, metrics, order_by, having

Note that a recipe contains data from a single table.`


---------------------------------
Viewing the data from your Recipe
---------------------------------

recipe.dataset.xxxx
iterating over recipe.all
dimensions have a separate _id property


-------
Caching
-------

-------------------------------------------
Running recipes in parallel with RecipePool
-------------------------------------------





----

Now, go check out the :ref:`API Documentation <api>` or begin
:ref:`Recipe Development <development>`.
