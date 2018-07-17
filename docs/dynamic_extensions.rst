.. _dynamic_extensions:

==================
Dynamic Extensions
==================

Recipes can to load dynamic plugins and extensions as hooks. The hooks
are expected to accept a recipe_parts dict or object and have an
execute method that returns a new recipe_parts dict or object. The
plugins must be in the appropiate namespace depending on where they get
called.  The `recipe.hooks.modify_query` namespace is one of the
namespaces that is available. You can see the ``recipe_caching``
library for a concrete implementation.


.. note::  Remember to use recipe's built in settings to handle any
           configuration options/settings you made need for your
           extension.

DynamicExtensionBase
====================

.. currentmodule:: recipe.dynamic_extensions
.. autoclass:: DynamicExtensionBase
   :inherited-members:
