import pytest
from sqlalchemy import func
from .test_base import *

from recipe import BadRecipe
from recipe import Dimension
from recipe import Metric
from recipe import Shelf, AutomaticShelf


class TestShelf(object):
    def setup(self):
        # create a Session
        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last),
            'age': Metric(func.sum(MyTable.age))
        })

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

        # We can choose not to raise
        ingredient = self.shelf.find('foo', Dimension, raise_if_invalid=False)
        assert ingredient == 'foo'

        ingredient = self.shelf.find(2.0, Dimension, raise_if_invalid=False)
        assert ingredient == 2.0

        ingredient = self.shelf.find('first', Metric, raise_if_invalid=False)
        assert ingredient == 'first'

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

        # We can choose not to raise
        ingredient = self.shelf.find('foo', Dimension, raise_if_invalid=False)
        assert ingredient == 'foo'

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
        ingredient = self.shelf.find('foo', Dimension, raise_if_invalid=False)
        assert ingredient == 'foo'

        ingredient = self.shelf.find(2.0, Dimension, raise_if_invalid=False)
        assert ingredient == 2.0

        ingredient = self.shelf.find('first', Metric, raise_if_invalid=False)
        assert ingredient == 'first'

    def test_get(self):
        """ Find ingredients on the shelf """
        print self.shelf
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

        # We can choose not to raise
        ingredient = self.shelf.find('foo', Dimension, raise_if_invalid=False)
        assert ingredient == 'foo'

        self.shelf['foo'] = Dimension(MyTable.last)
        ingredient = self.shelf.find('last', Dimension)
        assert ingredient.id == 'last'
