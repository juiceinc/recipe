========
Settings
========

Recipe now has a ``SETTINGS`` object that can be use to store and modify any
configuration options or setting needed for use within the recipe library.
Any library wide settings should be made in the ``default_settings.py`` file.
The settings object is based on the flapjack_stack library, which offers the
ability to store settings in a stack and easily pop on and off settings. You
can learn more about how to use flapjack_stack in its
`documentation <http://flapjack-stack.readthedocs.io/en/latest/>`_.

To access the ``SETTINGS`` object import it from recipe.

.. code-block:: python

    from recipe import SETTINGS
