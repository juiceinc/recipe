import pytest
from sqlalchemy import func

import recipe
from recipe import Dimension
from recipe import Metric
from recipe import Recipe
from recipe import Shelf
from recipe.extensions import RecipeExtension
from .test_base import *


def test_main():
    assert recipe  # use your library here


class DummyExtension(RecipeExtension):
    def a(self):
        return 'a'


class TestRecipeIngredients(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last),
            'age': Metric(func.sum(MyTable.age))
        })
        self.extension_classes = []

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session,
                      extension_classes=self.extension_classes)

    def test_call_extension_method(self):
        Recipe.extensions = []

        recipe = self.recipe().metrics('age').dimensions('first')

        with pytest.raises(AttributeError):
            value = recipe.a()

        with pytest.raises(AttributeError):
            recipe.b()

        self.extension_classes = [DummyExtension]
        recipe = self.recipe().metrics('age').dimensions('first')

        value = recipe.a()
        assert value == 'a'

        with pytest.raises(AttributeError):
            recipe.b()
