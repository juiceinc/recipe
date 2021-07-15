.. _api:

===
API
===


.. module:: recipe

This part of the documentation covers all the interfaces of Recipe.


------
Recipe
------


.. autoclass:: Recipe
   :inherited-members:


-----
Shelf
-----


.. autoclass:: Shelf
   :inherited-members:


-----------
Ingredients
-----------


.. autoclass:: Ingredient
   :members:

.. autoclass:: Dimension
   :members:

.. autoclass:: IdValueDimension
   :members:

.. autoclass:: Metric
   :members:

.. autoclass:: WtdAvgMetric
   :members:

.. autoclass:: DivideMetric
   :members:

.. autoclass:: Filter
   :members:

.. autoclass:: Having
   :members:


----------
Extensions
----------

.. autoclass:: RecipeExtension
    :members:

.. autoclass:: AutomaticFilters
    :members: apply_automatic_filters,automatic_filters,include_automatic_filter_keys,exclude_automatic_filter_keys

.. autoclass:: BlendRecipe
    :members: blend,full_blend

.. autoclass:: CompareRecipe
    :members: compare

.. autoclass:: SummarizeOver
    :members: summarize_over

.. autoclass:: Anonymize
    :members:

.. autoclass:: Paginate
    :members:

.. autoclass:: PaginateInline
    :members:

----------
Exceptions
----------

.. autoexception:: BadIngredient

.. autoexception:: BadRecipe


Now, go start some :ref:`Recipe Development <development>`.
