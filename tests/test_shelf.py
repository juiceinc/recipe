import pytest
from copy import copy
from sqlalchemy import func
from .test_base import *

from recipe import BadRecipe
from recipe import Dimension
from recipe import Metric
from recipe import Shelf, AutomaticShelf


class TestShelf(object):
    def setup(self):
        # create a Session
        self.shelf = copy(mytable_shelf)

    def test_find(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.find('first', Dimension)
        assert ingredient.id == 'first'

        # Raise if the wrong type
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('first', Metric)

        # Raise if key not present in shelf
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('foo', Dimension)

        # Raise if key is not an ingredient or string
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('foo', Dimension)

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('foo', Dimension)

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('foo', Dimension)

        self.shelf['foo'] = Dimension(MyTable.last)
        ingredient = self.shelf.find('last', Dimension)
        assert ingredient.id == 'last'

    def test_repr(self):
        """ Find ingredients on the shelf """
        assert self.shelf.__repr__() == """(Dimension)first MyTable.first
(Dimension)last MyTable.last
(Metric)age sum(foo.age)"""

    def test_brew(self):
        recipe_parts = self.shelf.brew_query_parts()
        assert len(recipe_parts['columns']) == 3
        assert len(recipe_parts['group_bys']) == 2
        assert len(recipe_parts['filters']) == 0
        assert len(recipe_parts['havings']) == 0

    def test_anonymize(self):
        """ We can save and store anonymization context """
        assert self.shelf.Meta.anonymize == False
        self.shelf.Meta.anonymize = True
        assert self.shelf.Meta.anonymize == True

    def test_get(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.first
        assert ingredient.id == 'first'

        ingredient = self.shelf.get('first', None)
        assert ingredient.id == 'first'

        ingredient = self.shelf.get('primo', None)
        assert ingredient is None

    def test_add_to_shelf(self):
        """ We can add an ingredient to a shelf """
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('foo', Dimension)

        self.shelf['foo'] = Dimension(MyTable.last)
        ingredient = self.shelf.find('last', Dimension)
        assert ingredient.id == 'last'

    def test_clear(self):
        assert len(self.shelf) == 3
        self.shelf.clear()
        assert len(self.shelf) == 0


class TestAutomaticShelf(object):
    def setup(self):
        self.shelf = AutomaticShelf(MyTable)

    def test_auto_find(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.find('first', Dimension)
        assert ingredient.id == 'first'

        # Raise if the wrong type
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('first', Metric)

        # Raise if key not present in shelf
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('foo', Dimension)

        # Raise if key is not an ingredient or string
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

        # We can choose not to raise
        # ingredient = self.shelf.find('foo', Dimension)
        # assert ingredient == 'foo'
        #
        # ingredient = self.shelf.find(2.0, Dimension)
        # assert ingredient == 2.0
        #
        # ingredient = self.shelf.find('first', Metric)
        # assert ingredient == 'first'

    def test_get(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.first
        assert ingredient.id == 'first'

        ingredient = self.shelf.get('first', None)
        assert ingredient.id == 'first'

        ingredient = self.shelf.get('primo', None)
        assert ingredient is None
