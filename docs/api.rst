.. _api:

===
API
===


.. module:: recipe

This part of the documentation covers all the interfaces of Recipe.


--------------
Recipe Object
--------------


.. autoclass:: Recipe
   :inherited-members:


------------
Shelf Object
------------


.. autoclass:: Shelf
   :inherited-members:

.. autoclass:: AutomaticShelf
   :inherited-members:


-----------
Ingredients
-----------


.. autoclass:: Ingredient
   :inherited-members:

.. autoclass:: Dimension
   :inherited-members:

.. autoclass:: Metric
   :inherited-members:



----------
Extensions
----------

.. autoclass:: RecipeExtension
    :members:

.. autoclass:: AutomaticFilters
    :members:

.. autoclass:: BlendRecipe
    :members:

.. autoclass:: CompareRecipe
    :members:

.. autoclass::SummarizeOver
    :members:

.. autoclass:: Anonymize
    :members:

----------
Exceptions
----------


.. autoexception:: BadIngredient

    You can't build an ingredient this way.


.. autoexception:: BadRecipe

    You can't build a recipe this way.


Now, go start some :ref:`Recipe Development <development>`.
