# -*- coding: utf-8 -*-

"""
Recipe
~~~~~~~~~~~~~~~~~~~~~
"""
import logging

from recipe.exceptions import BadIngredient, BadRecipe
from recipe.ingredients import (Ingredient, Dimension, LookupDimension,
                                IdValueDimension, Metric, DivideMetric,
                                WtdAvgMetric, CountIfMetric, SumIfMetric,
                                Filter, Having)
from recipe.core import Recipe
from recipe.shelf import Shelf, AutomaticShelf

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

__all__ = [BadIngredient, BadRecipe, Ingredient, Dimension, LookupDimension,
           IdValueDimension, Metric, DivideMetric, WtdAvgMetric, CountIfMetric,
           SumIfMetric, Filter, Having, Recipe, Shelf, AutomaticShelf]
