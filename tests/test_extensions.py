import pytest
from sqlalchemy import func

from recipe import Dimension
from recipe import Metric
from recipe import Recipe
from recipe import Shelf
from recipe.extensions import RecipeExtension, AutomaticFilters, \
    AnonymizeRecipe, \
    SummarizeOverRecipe
from .test_base import *


class DummyExtension(RecipeExtension):
    def a(self):
        return 'a'


class TestExtensions(object):
    def setup(self):
        # create a Session
        self.session = Session()
        self.shelf = mytable_shelf
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
    """ A simple extension that adds a filter """

    def add_ingedients(self):
        self.recipe.filters(MyTable.first > 2)


class TestAddFilterExtension(object):
    def setup(self):
        # create a Session
        self.session = Session()
        self.shelf = mytable_shelf
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
        self.shelf = mytable_shelf
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

    def test_apply(self):
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

    def test_automatic_filters(self):
        """ Automatic filters """
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

        with pytest.raises(AssertionError):
            # Automatic filters must be a dict
            recipe.automatic_filters(2)

    def test_apply_automatic_filters(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).apply_automatic_filters(False)
        assert recipe.recipe_extensions[0].dirty == True
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""

    def test_include_exclude_keys(self):
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

    def test_operators(self):
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


class TestAnonymizeRecipeExtension(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last,
                              # formatters=[lambda value: value[::-1]]),
                              anonymizer=lambda value: value[::-1]),
            'age': Metric(func.sum(MyTable.age))
        })
        self.extension_classes = [AnonymizeRecipe]

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session,
                      extension_classes=self.extension_classes)

    def test_apply(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe.anonymize(True)

        with pytest.raises(AssertionError):
            recipe.anonymize('pig')

    def test_anonymize_with_anonymizer(self):
        """ Anonymize requires ingredients to have an anonymizer """
        recipe = self.recipe().metrics('age').dimensions(
            'last').order_by('last').anonymize(False)
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.last AS last
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].last_id == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

        recipe = self.recipe().metrics('age').dimensions(
            'last').order_by('last').anonymize(True)
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.last AS last_raw
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'derf'
        assert recipe.all()[0].last_raw == 'fred'
        assert recipe.all()[0].last_id == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_anonymize_without_anonymizer(self):
        """ If the dimension doesn't have an anonymizer, there is no change """
        recipe = self.recipe().metrics('age').dimensions(
            'first').order_by('first').anonymize(False)
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first
ORDER BY foo.first"""
        assert recipe.all()[0].first == 'hi'
        assert recipe.all()[0].first_id == 'hi'
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

        recipe = self.recipe().metrics('age').dimensions(
            'first').order_by('first').anonymize(True)
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first
ORDER BY foo.first"""
        assert recipe.all()[0].first == 'hi'
        assert recipe.all()[0].first_id == 'hi'
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1


class TestSummarizeOverExtension(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last,
                              # formatters=[lambda value: value[::-1]]),
                              anonymizer=lambda value: value[::-1]),
            'age': Metric(func.sum(MyTable.age))
        })
        self.extension_classes = [SummarizeOverRecipe]

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session,
                      extension_classes=self.extension_classes)

    def test_summarize_over(self):
        """ Anonymize requires ingredients to have an anonymizer """
        recipe = self.recipe().metrics('age').dimensions(
            'first', 'last').summarize_over('last')
        assert recipe.to_sql() == """SELECT avg(summarized.age) AS age,
       summarized.first AS first
FROM
  (SELECT sum(foo.age) AS age,
          foo.last AS last,
          foo.first AS first
   FROM foo
   GROUP BY foo.last,
            foo.first) AS summarized
GROUP BY summarized.first"""
        assert len(recipe.all()) == 1
        assert recipe.one().first == 'hi'
        assert recipe.one().age == 7.5
