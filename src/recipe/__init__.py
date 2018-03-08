# -*- coding: utf-8 -*-
"""
Recipe
~~~~~~~~~~~~~~~~~~~~~
"""
import logging

from recipe.core import Recipe
from recipe.exceptions import BadIngredient
from recipe.exceptions import BadRecipe
from recipe.ingredients import Dimension
from recipe.ingredients import DivideMetric
from recipe.ingredients import Filter
from recipe.ingredients import Having
from recipe.ingredients import IdValueDimension
from recipe.ingredients import Ingredient
from recipe.ingredients import LookupDimension
from recipe.ingredients import Metric
from recipe.ingredients import WtdAvgMetric
from recipe.shelf import AutomaticShelf
from recipe.shelf import Shelf

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
