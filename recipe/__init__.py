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
from recipe.extensions import (
    Anonymize, AutomaticFilters, BlendRecipe, CompareRecipe, RecipeExtension,
    SummarizeOver
)
from recipe.ingredients import (
    Dimension, DivideMetric, Filter, Having, IdValueDimension, Ingredient,
    LookupDimension, Metric, WtdAvgMetric
)
from recipe.oven import get_oven
from recipe.shelf import AutomaticShelf, Shelf
from recipe.utils import FakerAnonymizer

SETTINGS = FlapjackStack()
SETTINGS.add_layer(default_settings)

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):

        def emit(self, record):
            pass


logging.getLogger(__name__).addHandler(NullHandler())

__version__ = '0.5.2'

__all__ = [
    'BadIngredient',
    'BadRecipe',
    'Ingredient',
    'Dimension',
    'LookupDimension',
    'IdValueDimension',
    'Metric',
    'DivideMetric',
    'WtdAvgMetric',
    'Filter',
    'Having',
    'Recipe',
    'Shelf',
    'AutomaticShelf',
    'SETTINGS',
    'get_oven',
    'Anonymize',
    'AutomaticFilters',
    'BlendRecipe',
    'CompareRecipe',
    'RecipeExtension',
    'SummarizeOver',
    'FakerAnonymizer',
]
