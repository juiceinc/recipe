# -*- coding: utf-8 -*-

"""
Recipe
~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2016 by Chris Gemignani.
:license: Apache 2.0, see LICENSE for more details.
"""

__title__ = 'recipe'
__version__ = '0.1.0'
__author__ = 'Chris Gemignani'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2016 Chris Gemignani'

# warnings.simplefilter('ignore', DependencyWarning)

import logging

from recipe.exceptions import BadIngredient, BadRecipe
from recipe.ingredients import Ingredient, Dimension, LookupDimension, \
    IdValueDimension, Metric, DivideMetric, CountIfMetric, SumIfMetric, \
    SimpleMetric
from recipe.core import Recipe
from recipe.shelf import Shelf

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
