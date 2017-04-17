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


.. autoclass:: Shelf
   :inherited-members:



---------
Functions
---------


.. autofunction:: detect

.. autofunction:: import_set


----------
Exceptions
----------


.. class:: InvalidDatasetType

    You're trying to add something that doesn't quite look right.


.. class:: InvalidDimensions

    You're trying to add something that doesn't quite fit right.


.. class:: UnsupportedFormat

    You're trying to add something that doesn't quite taste right.


Now, go start some :ref:`Tablib Development <development>`.
