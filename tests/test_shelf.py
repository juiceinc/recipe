from copy import copy

import pytest
from tests.test_base import MyTable, mytable_shelf

from recipe import AutomaticShelf, BadRecipe, Dimension, Metric, Shelf


class TestShelf(object):

    def setup(self):
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

    def test_update(self):
        """ Shelves can be updated with other shelves """
        new_shelf = Shelf({
            'squee': Dimension(MyTable.first),
        })
        assert len(self.shelf) == 3
        self.shelf.update(new_shelf)
        assert len(self.shelf) == 4

    def test_update_key_value(self):
        """ Shelves can be built with key_values and updated """
        new_shelf = Shelf(squee=Dimension(MyTable.first))
        assert len(self.shelf) == 3
        self.shelf.update(new_shelf)
        assert len(self.shelf) == 4
        assert isinstance(self.shelf.get('squee'), Dimension)

    def test_update_key_value_direct(self):
        """ Shelves can be updated directly with key_value"""
        assert len(self.shelf) == 3
        self.shelf.update(squee=Dimension(MyTable.first))
        assert len(self.shelf) == 4
        assert isinstance(self.shelf.get('squee'), Dimension)

    def test_brew(self):
        recipe_parts = self.shelf.brew_query_parts()
        assert len(recipe_parts['columns']) == 3
        assert len(recipe_parts['group_bys']) == 2
        assert len(recipe_parts['filters']) == 0
        assert len(recipe_parts['havings']) == 0

    def test_anonymize(self):
        """ We can save and store anonymization context """
        assert self.shelf.Meta.anonymize is False
        self.shelf.Meta.anonymize = True
        assert self.shelf.Meta.anonymize is True

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

    def test_dimension_ids(self):
        assert len(self.shelf.dimension_ids) == 2
        assert self.shelf.dimension_ids in (('last', 'first'),
                                            ('first', 'last'))

    def test_metric_ids(self):
        assert len(self.shelf.metric_ids) == 1
        assert self.shelf.metric_ids == ('age',)


class TestShelfFromYaml(object):

    def setup(self):
        self.shelf = Shelf.from_yaml(
            """
first:
    kind: Dimension
    field: first
last:
    kind: Dimension
    field: last
age:
    kind: Metric
    field: age
""", MyTable
        )
        self.shelf.Meta.anonymize = False

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

    def test_update(self):
        """ Shelves can be updated with other shelves """
        new_shelf = Shelf({
            'squee': Dimension(MyTable.first),
        })
        assert len(self.shelf) == 3
        self.shelf.update(new_shelf)
        assert len(self.shelf) == 4

    def test_update_key_value(self):
        """ Shelves can be built with key_values and updated """
        new_shelf = Shelf(squee=Dimension(MyTable.first))
        assert len(self.shelf) == 3
        self.shelf.update(new_shelf)
        assert len(self.shelf) == 4
        assert isinstance(self.shelf.get('squee'), Dimension)

    def test_update_key_value_direct(self):
        """ Shelves can be updated directly with key_value"""
        assert len(self.shelf) == 3
        self.shelf.update(squee=Dimension(MyTable.first))
        assert len(self.shelf) == 4
        assert isinstance(self.shelf.get('squee'), Dimension)

    def test_brew(self):
        recipe_parts = self.shelf.brew_query_parts()
        assert len(recipe_parts['columns']) == 3
        assert len(recipe_parts['group_bys']) == 2
        assert len(recipe_parts['filters']) == 0
        assert len(recipe_parts['havings']) == 0

    def test_anonymize(self):
        """ We can save and store anonymization context """
        assert self.shelf.Meta.anonymize is False
        self.shelf.Meta.anonymize = True
        assert self.shelf.Meta.anonymize is True

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

    def test_get(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.first
        assert ingredient.id == 'first'

        ingredient = self.shelf.get('first', None)
        assert ingredient.id == 'first'

        ingredient = self.shelf.get('primo', None)
        assert ingredient is None
