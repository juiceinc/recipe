# -*- coding: utf-8 -*-
"""
Recipe
~~~~~~~~~~~~~~~~~~~~~
"""
import logging

from flapjack_stack import FlapjackStack

from recipe import default_settings
from recipe.core import Recipe
from recipe.exceptions import BadIngredient
from recipe.exceptions import BadRecipe
from recipe.ingredients import CountIfMetric
from recipe.ingredients import Dimension
from recipe.ingredients import DivideMetric
from recipe.ingredients import Filter
from recipe.ingredients import Having
from recipe.ingredients import IdValueDimension
from recipe.ingredients import Ingredient
from recipe.ingredients import LookupDimension
from recipe.ingredients import Metric
from recipe.ingredients import SumIfMetric
from recipe.ingredients import WtdAvgMetric
from recipe.oven import get_oven
from recipe.shelf import AutomaticShelf
from recipe.shelf import Shelf

SETTINGS = FlapjackStack()
SETTINGS.add_layer(default_settings)

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):

        def emit(self, record):
            pass


logging.getLogger(__name__).addHandler(NullHandler())

__all__ = [
    'BadIngredient', 'BadRecipe', 'Ingredient', 'Dimension', 'LookupDimension',
    'IdValueDimension', 'Metric', 'DivideMetric', 'WtdAvgMetric',
    'CountIfMetric', 'SumIfMetric', 'Filter', 'Having', 'Recipe', 'Shelf',
    'AutomaticShelf', 'SETTINGS', 'get_oven'
]
