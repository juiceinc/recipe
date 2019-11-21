========
Settings
========

Recipe now has a ``SETTINGS`` object that can be use to store and modify any
configuration options or setting needed for use within the recipe library.

To access the ``SETTINGS`` object import it from recipe.

.. code-block:: python

    from recipe import SETTINGS

Recipe settings are

**POOL_SIZE**
    Used to set the ``pool_size`` kwarg property in a SQLAlchemy connection

**POOL_RECYCLE**
    Used to set the ``pool_recycle`` kwarg property in a SQLAlchemy connection

The pluggable recipe_caching extension uses the following setting.

**CACHE_REGIONS**
    Used to set a dictionary of dogpile cache regions.
