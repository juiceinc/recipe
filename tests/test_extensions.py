from copy import copy

import pytest

from recipe import BadRecipe
from recipe import Recipe
from recipe.extensions import RecipeExtension, AutomaticFilters, \
    AnonymizeRecipe, \
    SummarizeOverRecipe, CompareRecipe, BlendRecipe
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

    def add_ingredients(self):
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
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
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

        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.apply_automatic_filters(True)

        assert recipe.recipe_extensions[0].apply == True

        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""

    def test_automatic_filters(self):
        """ Automatic filters """
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        })

        assert recipe.recipe_extensions[0].apply == True
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
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
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""

    def test_include_exclude_keys(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).include_automatic_filter_keys('foo')
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).include_automatic_filter_keys('foo', 'first')
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.first IN ('foo')
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).exclude_automatic_filter_keys('foo')
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.first IN ('foo')
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).exclude_automatic_filter_keys('first')
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first': ['foo']
        }).exclude_automatic_filter_keys('first')
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""

    def test_operators(self):
        # Testing operators
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first__notin': ['foo']
        })
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.first NOT IN ('foo')
GROUP BY foo.first"""

        # between operator
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first__between': ['foo', 'moo']
        })
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.first BETWEEN 'foo' AND 'moo'
GROUP BY foo.first"""

        # scalar operator
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({
            'first__lt': 'moo'
        })
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
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
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].last_id == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

        recipe = self.recipe().metrics('age').dimensions(
            'last').order_by('last').anonymize(True)
        assert recipe.to_sql() == """SELECT foo.last AS last_raw,
       sum(foo.age) AS age
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
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first
ORDER BY foo.first"""
        assert recipe.all()[0].first == 'hi'
        assert recipe.all()[0].first_id == 'hi'
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

        recipe = self.recipe().metrics('age').dimensions(
            'first').order_by('first').anonymize(True)
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
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
        assert recipe.to_sql() == """SELECT summarized.first AS first,
       avg(summarized.age) AS age
FROM
  (SELECT foo.first AS first,
          foo.last AS last,
          sum(foo.age) AS age
   FROM foo
   GROUP BY foo.first,
            foo.last) AS summarized
GROUP BY summarized.first"""
        assert len(recipe.all()) == 1
        assert recipe.one().first == 'hi'
        assert recipe.one().age == 7.5


class TestCompareRecipeExtension(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = copy(census_shelf)
        self.extension_classes = [CompareRecipe]

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session,
                      extension_classes=self.extension_classes)

    def test_compare(self):
        """ A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics('pop2000').dimensions(
            'sex').order_by('sex')
        r = r.compare(self.recipe() \
                      .metrics('pop2000')
                      .dimensions('sex')
                      .filters(Census.state == 'Vermont'))

        assert len(r.all()) == 2
        assert r.to_sql() == """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       anon_1.pop2000 AS pop2000_compare
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY census.sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex"""
        rowwomen, rowmen = r.all()[0], r.all()[1]
        # We should get the lookup values
        assert rowwomen.sex == 'F'
        assert rowwomen.pop2000 == 143534804
        assert rowwomen.pop2000_compare == 310948
        assert rowmen.sex == 'M'
        assert rowmen.pop2000 == 137392517
        assert rowmen.pop2000_compare == 298532

    def test_compare_suffix(self):
        """ Test that the proper suffix gets added to the comparison metrics
        """

        r = self.recipe().metrics('pop2000').dimensions(
            'sex').order_by('sex')
        r = r.compare(self.recipe()
                      .metrics('pop2000')
                      .dimensions('sex')
                      .filters(Census.state == 'Vermont'),
                      suffix='_x')

        assert len(r.all()) == 2
        assert r.to_sql() == """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       anon_1.pop2000 AS pop2000_x
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY census.sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex"""
        rowwomen, rowmen = r.all()[0], r.all()[1]
        # The comparison metric is named with the suffix
        assert rowwomen.sex == 'F'
        assert rowwomen.pop2000 == 143534804
        assert rowwomen.pop2000_x == 310948
        assert not hasattr(rowwomen, 'pop2000_compare')
        assert rowmen.sex == 'M'
        assert rowmen.pop2000 == 137392517
        assert rowmen.pop2000_x == 298532
        assert not hasattr(rowmen, 'pop2000_compare')

    def test_multiple_compares(self):
        """ Test that we can do multiple comparisons
        """

        r = self.recipe().metrics('pop2000').dimensions(
            'sex', 'state').order_by('sex', 'state')
        r = r.compare(self.recipe().metrics('pop2000').dimensions('sex')
                      .filters(Census.state == 'Vermont'),
                      suffix='_vermont')
        r = r.compare(self.recipe().metrics('pop2000'),
                      suffix='_total')

        assert len(r.all()) == 102
        assert r.to_sql() == """SELECT census.sex AS sex,
       census.state AS state,
       sum(census.pop2000) AS pop2000,
       anon_1.pop2000 AS pop2000_vermont,
       anon_2.pop2000 AS pop2000_total
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY census.sex) AS anon_1 ON census.sex = anon_1.sex
LEFT OUTER JOIN
  (SELECT sum(census.pop2000) AS pop2000
   FROM census) AS anon_2 ON 1=1
GROUP BY census.sex,
         census.state
ORDER BY census.sex,
         census.state"""

        alabama_women, alaska_women = r.all()[0], r.all()[1]
        assert alabama_women.sex == 'F'
        assert alabama_women.pop2000 == 2300612
        assert alabama_women.pop2000_vermont == 310948
        assert alabama_women.pop2000_total == 280927321
        assert not hasattr(alabama_women, 'pop2000_compare')
        assert alaska_women.sex == 'F'
        assert alaska_women.pop2000 == 300043
        assert alaska_women.pop2000_vermont == 310948
        assert alaska_women.pop2000_total == 280927321
        assert not hasattr(alaska_women, 'pop2000_compare')

    def test_mismatched_dimensions_raises(self):
        """ Dimensions in the comparison recipe must be a subset of the
        dimensions in the base recipe """
        r = self.recipe().metrics('pop2000').dimensions(
            'sex').order_by('sex')
        r = r.compare(self.recipe()
                      .metrics('pop2000')
                      .dimensions('state')
                      .filters(Census.state == 'Vermont'),
                      suffix='_x')

        with pytest.raises(BadRecipe):
            r.all()


class TestBlendRecipeExtension(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = copy(census_shelf)
        self.extension_classes = [BlendRecipe]

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session,
                      extension_classes=self.extension_classes)

    def test_self_blend(self):
        """ A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics('pop2000').dimensions(
            'sex').order_by('sex')

        blend_recipe = self.recipe() \
            .metrics('pop2008') \
            .dimensions('sex') \
            .filters(Census.sex == 'F')
        r = r.full_blend(blend_recipe, join_base='sex',
                         join_blend='sex')

        assert len(r.all()) == 2
        assert r.to_sql() == """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       anon_1.pop2008 AS pop2008
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2008) AS pop2008
   FROM census
   WHERE census.sex = 'F'
   GROUP BY census.sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex"""
        rowwomen, rowmen = r.all()[0], r.all()[1]
        # We should get the lookup values
        assert rowwomen.sex == 'F'
        assert rowwomen.pop2000 == 143534804
        assert rowwomen.pop2008 == 153959198
        assert rowmen.sex == 'M'
        assert rowmen.pop2000 == 137392517
        assert rowmen.pop2008 == None

    def test_blend(self):
        """ A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics('pop2000').dimensions(
            'state').order_by('state')

        blend_recipe = self.recipe().shelf(statefact_shelf) \
            .dimensions('state', 'abbreviation')
        r = r.blend(blend_recipe, join_base='state',
                    join_blend='state')

        assert r.to_sql() == """SELECT census.state AS state,
       sum(census.pop2000) AS pop2000,
       anon_1.abbreviation AS abbreviation
FROM census
JOIN
  (SELECT state_fact.abbreviation AS abbreviation,
          state_fact.name AS state
   FROM state_fact
   GROUP BY state_fact.abbreviation,
            state_fact.name) AS anon_1 ON census.state = anon_1.state
GROUP BY census.state,
         anon_1.abbreviation
ORDER BY census.state"""

        assert len(r.all()) == 50
        alabamarow, alaskarow = r.all()[0], r.all()[1]
        assert alabamarow.state == 'Alabama'
        assert alabamarow.state_id == 'Alabama'
        assert alabamarow.abbreviation == 'AL'
        assert alabamarow.abbreviation_id == 'AL'
        assert alabamarow.pop2000 == 4438559
        assert alaskarow.state == 'Alaska'
        assert alaskarow.state_id == 'Alaska'
        assert alaskarow.abbreviation == 'AK'
        assert alaskarow.abbreviation_id == 'AK'
        assert alaskarow.pop2000 == 608588
