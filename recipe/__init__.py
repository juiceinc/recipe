# -*- coding: utf-8 -*-

"""
Recipe
~~~~~~~~~~~~~~~~~~~~~
"""
import logging

from flapjack_stack import FlapjackStack

from recipe import default_settings
from recipe.core import Recipe
from recipe.exceptions import BadIngredient, BadRecipe
from recipe.ingredients import (Ingredient, Dimension, LookupDimension,
                                IdValueDimension, Metric, DivideMetric,
                                WtdAvgMetric, CountIfMetric, SumIfMetric,
                                Filter, Having)
from recipe.oven import get_oven
from recipe.shelf import Shelf, AutomaticShelf

SETTINGS = FlapjackStack()
SETTINGS.add_layer(default_settings)

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

__all__ = ['BadIngredient', 'BadRecipe', 'Ingredient', 'Dimension',
           'LookupDimension', 'IdValueDimension', 'Metric', 'DivideMetric',
           'WtdAvgMetric', 'CountIfMetric', 'SumIfMetric', 'Filter',
           'Having', 'Recipe', 'Shelf', 'AutomaticShelf', 'SETTINGS',
           'get_oven']
