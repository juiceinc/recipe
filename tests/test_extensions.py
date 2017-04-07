import pytest
from sqlalchemy import func

from recipe import Dimension
from recipe import Metric
from recipe import Recipe
from recipe import Shelf
from recipe.extensions import RecipeExtension, AutomaticFilters
from .test_base import *


class DummyExtension(RecipeExtension):
    def a(self):
        return 'a'


class TestExtensions(object):
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


class AddFilter(RecipeExtension):
    def add_ingedients(self):
        self.recipe.filters(MyTable.first > 2)


class TestAddFilterExtension(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last),
            'age': Metric(func.sum(MyTable.age))
        })
        self.extension_classes = [AddFilter]

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session,
                      extension_classes=self.extension_classes)

    def test_add_filter(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
WHERE foo.first > 2
GROUP BY foo.first"""


class TestAutomaticFiltersExtension(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last),
            'age': Metric(func.sum(MyTable.age))
        })
        self.extension_classes = [AutomaticFilters]

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session,
                      extension_classes=self.extension_classes)

    def test_proxy_calls(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.apply_automatic_filters(False)

        assert recipe.recipe_extensions[0].apply == False
        assert recipe.recipe_extensions[0].dirty == True

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.include_automatic_filter_keys('first')
        assert recipe.recipe_extensions[0].include_keys == ('first',)

        # Test chaining
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.include_automatic_filter_keys(
            'first').exclude_automatic_filter_keys('last')
        assert recipe.recipe_extensions[0].include_keys == ('first',)
        assert recipe.recipe_extensions[0].exclude_keys == ('last',)

    def test_automatic_filters(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.apply_automatic_filters(False)

        assert recipe.recipe_extensions[0].apply == False

        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.apply_automatic_filters(True)

        assert recipe.recipe_extensions[0].apply == True

        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        })

        assert recipe.recipe_extensions[0].apply == True
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
WHERE foo.first IN ('foo')
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).apply_automatic_filters(False)
        assert recipe.recipe_extensions[0].dirty == True
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).include_automatic_filter_keys('foo')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).include_automatic_filter_keys('foo', 'first')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
WHERE foo.first IN ('foo')
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).exclude_automatic_filter_keys('foo')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
WHERE foo.first IN ('foo')
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).exclude_automatic_filter_keys('first')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).exclude_automatic_filter_keys('first')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""

        # Testing operators
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first__notin': ['foo']
        })
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
WHERE foo.first NOT IN ('foo')
GROUP BY foo.first"""

        # between operator
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first__between': ['foo', 'moo']
        })
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
WHERE foo.first BETWEEN 'foo' AND 'moo'
GROUP BY foo.first"""

        # scalar operator
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first__lt': 'moo'
        })
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
WHERE foo.first < 'moo'
GROUP BY foo.first"""




