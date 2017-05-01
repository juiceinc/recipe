.. _api:

===
API
===


.. module:: recipe

This part of the documentation covers all the interfaces of Tablib.  For
parts where Tablib depends on external libraries, we document the most
important right here and provide links to the canonical documentation.


--------------
Recipe Object
--------------


.. autoclass:: Recipe
   :inherited-members:


---------------
Shelf Object
---------------


.. autoclass:: Shelf
   :inherited-members:

.. autoclass:: AutomaticShelf
   :inherited-members:


-----------------
Ingredient Object
-----------------


.. autoclass:: Ingredient
   :inherited-members:



---------
Functions
---------




----------
Exceptions
----------


.. class:: BadIngredient

    You can't build an ingredient this way.


.. class:: BadRecipe

    You can't build a recipe this way.


Now, go start some :ref:`Recipe Development <development>`.
