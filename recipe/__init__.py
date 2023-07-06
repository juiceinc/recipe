# -*- coding: utf-8 -*-
"""
Recipe
~~~~~~~~~~~~~~~~~~~~~
"""
import logging

from recipe.core import Recipe
from recipe.exceptions import BadIngredient, BadRecipe, InvalidColumnError
from recipe.extensions import (
    Anonymize,
    AutomaticFilters,
    BlendRecipe,
    CompareRecipe,
    RecipeExtension,
    SummarizeOver,
    Paginate,
    PaginateInline,
    PaginateCountOver,
)
from recipe.ingredients import (
    Dimension,
    DivideMetric,
    Filter,
    Having,
    IdValueDimension,
    Ingredient,
    InvalidIngredient,
    LookupDimension,
    Metric,
    WtdAvgMetric,
)
from recipe.shelf import AutomaticShelf, Shelf
from recipe.utils import FakerAnonymizer


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


logging.getLogger(__name__).addHandler(NullHandler())

__version__ = "0.35.4"

__all__ = [
    "BadIngredient",
    "BadRecipe",
    "InvalidColumnError",
    "Ingredient",
    "Dimension",
    "LookupDimension",
    "IdValueDimension",
    "Metric",
    "DivideMetric",
    "WtdAvgMetric",
    "InvalidIngredient",
    "Filter",
    "Having",
    "Recipe",
    "Shelf",
    "AutomaticShelf",
    "Anonymize",
    "AutomaticFilters",
    "BlendRecipe",
    "CompareRecipe",
    "RecipeExtension",
    "Paginate",
    "PaginateInline",
    "PaginateCountOver",
    "FakerAnonymizer",
]
