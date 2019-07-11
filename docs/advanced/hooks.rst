=====
Hooks
=====

Recipes can call optional hooks to modify the recipe as it progresses
towards execution. This is done by add the desired hooks names to the
``dynamic_extensions`` property of the recipe. Currently, no hooks are
implemented in the base recipe library. However, much like ovens, they
can be loaded via third party libraries.

For example, if we installed the ``recipe_caching`` library, we could
add it's extension as shown here:

.. code-block:: python

    Recipe(shelf=shelf, session=oven.Session(), dynamic_extensions=['caching'])

You can learn more about creating your own in the :ref:`dynamic_extensions`
section.
