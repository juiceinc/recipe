================================
Using Shelves from configuration
================================

When are Shelves from configuration bound to columns?
-----------------------------------------------------

Shelf configuration can be **bound** at any time to a selectable. This can 
be any one of:

* A SQLAlchemy Mapping
* A SQLAlchemy subselect
* A Recipe

Binding a shelf to a Mapping
----------------------------

Binding shelves to Mappings is the most common usage of shelves. 
It connects the shelf config to database table columns.

Let's look at an example of binding a shelf to a Mapping.

.. code:: python

    Create simple census shelf
    Average age by state
    Get min/max average ages

The results look like this:

.. code:: 

    dfs



Binding a shelf to a SQLAlchemy subselect
-----------------------------------------

Binding shelves to Mappings is the most common usage of shelves. 
It connects the shelf config to database table columns.

Let's look at an example of binding a shelf to a Mapping.

.. code:: python

    Create a subselect that joins the table to additional data
    

The results look like this:

.. code:: 

    dfs
    


Binding a shelf to a Recipe
---------------------------




