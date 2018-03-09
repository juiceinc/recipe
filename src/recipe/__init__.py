# -*- coding: utf-8 -*-
"""
Recipe
~~~~~~~~~~~~~~~~~~~~~
"""
import logging

from recipe.core import Recipe
from recipe.exceptions import BadIngredient, BadRecipe
from recipe.ingredients import (
    Dimension, DivideMetric, Filter, Having, IdValueDimension, Ingredient,
    LookupDimension, Metric, WtdAvgMetric
)
from recipe.shelf import AutomaticShelf, Shelf

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):

        def emit(self, record):
            pass


logging.getLogger(__name__).addHandler(NullHandler())

__all__ = [
    BadIngredient, BadRecipe, Ingredient, Dimension, LookupDimension,
    IdValueDimension, Metric, DivideMetric, WtdAvgMetric, Filter, Having,
    Recipe, Shelf, AutomaticShelf
]
