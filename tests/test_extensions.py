from copy import copy

import pytest
from sqlalchemy import func
from tests.test_base import (
    Census, MyTable, census_shelf, mytable_shelf, oven, scores_shelf,
    statefact_shelf, tagscores_shelf
)

from recipe import BadRecipe, Dimension, Metric, Recipe, Shelf
from recipe.extensions import (
    Anonymize, AutomaticFilters, BlendRecipe, CompareRecipe, RecipeExtension,
    SummarizeOver
)


class DummyExtension(RecipeExtension):

    def a(self):
        return 'a'


class TestExtensions(object):

    def setup(self):
        # create a Session
        self.session = oven.Session()
        self.shelf = mytable_shelf
        self.extension_classes = []

    def recipe(self):
        return Recipe(
            shelf=self.shelf,
            session=self.session,
            extension_classes=self.extension_classes
        )

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
        self.session = oven.Session()
        self.shelf = mytable_shelf
        self.extension_classes = [AddFilter]

    def recipe(self):
        return Recipe(
            shelf=self.shelf,
            session=self.session,
            extension_classes=self.extension_classes
        )

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
        self.session = oven.Session()
        self.shelf = mytable_shelf
        self.extension_classes = [AutomaticFilters]

    def recipe(self):
        return Recipe(
            shelf=self.shelf,
            session=self.session,
            extension_classes=self.extension_classes
        )

    def test_proxy_calls(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.apply_automatic_filters(False)

        assert recipe.recipe_extensions[0].apply is False
        assert recipe.recipe_extensions[0].dirty is True

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.include_automatic_filter_keys('first')
        assert recipe.recipe_extensions[0].include_keys == ('first',)

        # Test chaining
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.include_automatic_filter_keys(
            'first'
        ).exclude_automatic_filter_keys('last')
        assert recipe.recipe_extensions[0].include_keys == ('first',)
        assert recipe.recipe_extensions[0].exclude_keys == ('last',)

    def test_apply(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.apply_automatic_filters(False)

        assert recipe.recipe_extensions[0].apply is False

        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""

        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.apply_automatic_filters(True)

        assert recipe.recipe_extensions[0].apply is True

        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""

    def test_automatic_filters(self):
        """ Automatic filters """
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({'first': ['foo']})

        assert recipe.recipe_extensions[0].apply is True
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
        assert recipe.recipe_extensions[0].dirty is True
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
        recipe = recipe.automatic_filters({'first__notin': ['foo']})
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.first NOT IN ('foo')
GROUP BY foo.first"""

        # between operator
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({'first__between': ['foo', 'moo']})
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.first BETWEEN 'foo' AND 'moo'
GROUP BY foo.first"""

        # scalar operator
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe = recipe.automatic_filters({'first__lt': 'moo'})
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.first < 'moo'
GROUP BY foo.first"""


class TestAnonymizeRecipeExtension(object):

    def setup(self):
        # create a Session
        self.session = oven.Session()

        self.shelf = Shelf({
            'first':
                Dimension(MyTable.first),
            'last':
                Dimension(
                    MyTable.last,
                    # formatters=[lambda value: value[::-1]]),
                    anonymizer=lambda value: value[::-1]
                ),
            'age':
                Metric(func.sum(MyTable.age))
        })
        self.extension_classes = [Anonymize]

    def recipe(self):
        return Recipe(
            shelf=self.shelf,
            session=self.session,
            extension_classes=self.extension_classes
        )

    def test_apply(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        recipe.anonymize(True)

        with pytest.raises(AssertionError):
            recipe.anonymize('pig')

    def test_anonymize_with_anonymizer(self):
        """ Anonymize requires ingredients to have an anonymizer """
        recipe = self.recipe(
        ).metrics('age').dimensions('last').order_by('last').anonymize(False)
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].last_id == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

        recipe = self.recipe(
        ).metrics('age').dimensions('last').order_by('last').anonymize(True)
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
        recipe = self.recipe(
        ).metrics('age').dimensions('first').order_by('first').anonymize(False)
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first
ORDER BY foo.first"""
        assert recipe.all()[0].first == 'hi'
        assert recipe.all()[0].first_id == 'hi'
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

        recipe = self.recipe(
        ).metrics('age').dimensions('first').order_by('first').anonymize(True)
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
    anonymized_foo_shelf = Shelf({
        'first':
            Dimension(MyTable.first, anonymizer=lambda value: value[::-1]),
        'last':
            Dimension(MyTable.last, anonymizer=lambda value: value[::-1]),
        'age':
            Metric(func.sum(MyTable.age))
    })

    def setup(self):
        # create a Session
        self.session = oven.Session()
        self.extension_classes = [SummarizeOver, Anonymize, AutomaticFilters]

    def recipe(self):
        return Recipe(
            shelf=self.shelf,
            session=self.session,
            extension_classes=self.extension_classes
        )

    def test_summarize_over(self):
        """ Anonymize requires ingredients to have an anonymizer """
        self.shelf = self.anonymized_foo_shelf
        recipe = self.recipe().metrics('age').dimensions(
            'first', 'last'
        ).summarize_over('last')
        assert recipe.to_sql() == """SELECT summarize.first,
       sum(summarize.age) AS age
FROM
  (SELECT foo.first AS first,
          foo.last AS last,
          sum(foo.age) AS age
   FROM foo
   GROUP BY foo.first,
            foo.last) AS summarize
GROUP BY summarize.first"""
        assert len(recipe.all()) == 1
        assert recipe.one().first == 'hi'
        assert recipe.one().age == 15

    def test_summarize_over_anonymize(self):
        """ Anonymize requires ingredients to have an anonymizer """
        self.shelf = self.anonymized_foo_shelf
        recipe = self.recipe().metrics('age').dimensions(
            'first', 'last'
        ).summarize_over('last').anonymize(True)
        assert recipe.to_sql() == """SELECT summarize.first_raw,
       sum(summarize.age) AS age
FROM
  (SELECT foo.first AS first_raw,
          foo.last AS last_raw,
          sum(foo.age) AS age
   FROM foo
   GROUP BY foo.first,
            foo.last) AS summarize
GROUP BY summarize.first_raw"""
        assert len(recipe.all()) == 1
        assert recipe.one().first == 'ih'
        assert recipe.one().age == 15

    ####
    # Scores is a dataset containing multiple tests that each user
    # has taken, we want to show the average USER score by department
    ####

    def test_summarize_over_scores(self):
        """ Test a dataset that has multiple rows per user """
        self.shelf = scores_shelf
        recipe = self.recipe().metrics('score').dimensions(
            'department', 'username'
        ).summarize_over('username')
        assert recipe.to_sql() == """SELECT summarize.department,
       avg(summarize.score) AS score
FROM
  (SELECT scores.department AS department,
          scores.username AS username,
          avg(scores.score) AS score
   FROM scores
   GROUP BY scores.department,
            scores.username) AS summarize
GROUP BY summarize.department"""
        ops_row, sales_row = recipe.all()
        assert ops_row.department == 'ops'
        assert ops_row.score == 87.5
        assert sales_row.department == 'sales'
        assert sales_row.score == 80.0

    def test_summarize_over_scores_limit(self):
        """ Test that limits and offsets work """
        self.shelf = scores_shelf

        recipe = self.recipe().metrics('score').dimensions(
            'department', 'username'
        ).summarize_over('username').limit(2)

        print('=' * 80)
        print('=' * 80)
        print(recipe.to_sql())
        print('=' * 80)
        print('=' * 80)
        assert recipe.to_sql() in (
            """SELECT summarize.department,
       avg(summarize.score) AS score
FROM
  (SELECT scores.department AS department,
          scores.username AS username,
          avg(scores.score) AS score
   FROM scores
   GROUP BY scores.department,
            scores.username) AS summarize
GROUP BY summarize.department LIMIT 2
OFFSET 0""", """SELECT summarize.department,
       avg(summarize.score) AS score
FROM
  (SELECT scores.department AS department,
          scores.username AS username,
          avg(scores.score) AS score
   FROM scores
   GROUP BY scores.department,
            scores.username) AS summarize
GROUP BY summarize.department
LIMIT 2
OFFSET 0"""
        )
        ops_row, sales_row = recipe.all()
        assert ops_row.department == 'ops'
        assert ops_row.score == 87.5
        assert sales_row.department == 'sales'
        assert sales_row.score == 80.0

        def test_summarize_over_scores_order(self):
            """ Order bys are hoisted to the outer query """
            self.shelf = scores_shelf

            recipe = self.recipe().metrics('score').dimensions(
                'department', 'username'
            ).summarize_over('username').order_by('department')

            assert recipe.to_sql() == """SELECT summarize.department,
           avg(summarize.score) AS score
    FROM
      (SELECT scores.department AS department,
              scores.username AS username,
              avg(scores.score) AS score
       FROM scores
       GROUP BY scores.department,
                scores.username
       ORDER BY scores.department) AS summarize
    GROUP BY summarize.department
    ORDER BY summarize.department"""
            ops_row, sales_row = recipe.all()
            assert ops_row.department == 'ops'
            assert ops_row.score == 87.5
            assert sales_row.department == 'sales'
            assert sales_row.score == 80.0

    def test_summarize_over_scores_order_anonymize(self):
        """ Order bys are hoisted to the outer query """
        self.shelf = scores_shelf

        recipe = self.recipe().metrics('score').dimensions(
            'department', 'username'
        ).summarize_over('username').order_by('department').anonymize(True)

        assert recipe.to_sql() == """SELECT summarize.department_raw,
       avg(summarize.score) AS score
FROM
  (SELECT scores.department AS department_raw,
          scores.username AS username,
          avg(scores.score) AS score
   FROM scores
   GROUP BY scores.department,
            scores.username
   ORDER BY scores.department) AS summarize
GROUP BY summarize.department_raw
ORDER BY summarize.department_raw"""
        ops_row, sales_row = recipe.all()
        assert ops_row.department == 'spo'
        assert ops_row.score == 87.5
        assert sales_row.department == 'selas'
        assert sales_row.score == 80.0

    def test_summarize_over_scores_automatic_filters(self):
        """ Test that automatic filters take place in the subquery """
        self.shelf = scores_shelf

        recipe = self.recipe().metrics('score').dimensions(
            'department', 'username'
        ).automatic_filters({
            'department': 'ops'
        }).summarize_over('username').anonymize(False)

        assert recipe.to_sql() == """SELECT summarize.department,
       avg(summarize.score) AS score
FROM
  (SELECT scores.department AS department,
          scores.username AS username,
          avg(scores.score) AS score
   FROM scores
   WHERE scores.department = 'ops'
   GROUP BY scores.department,
            scores.username) AS summarize
GROUP BY summarize.department"""
        ops_row = recipe.one()
        assert ops_row.department == 'ops'
        assert ops_row.score == 87.5

    ####
    # TagScores is a dataset containing multiple tests that each user
    # has taken, we want to show the average USER score by department
    # Users also have tags that we may want to limit to
    ####

    def test_summarize_over_tagscores(self):
        """ Test a dataset that has multiple rows per user """
        self.shelf = tagscores_shelf
        recipe = self.recipe().metrics('score').dimensions(
            'department', 'username'
        ).summarize_over('username')

        assert recipe.to_sql() == """SELECT summarize.department,
       sum(summarize.score) AS score
FROM
  (SELECT tagscores.department AS department,
          tagscores.username AS username,
          avg(tagscores.score) AS score
   FROM tagscores
   GROUP BY tagscores.department,
            tagscores.username) AS summarize
GROUP BY summarize.department"""
        ops_row, sales_row = recipe.all()
        assert ops_row.department == 'ops'
        assert ops_row.score == 175.0
        assert sales_row.department == 'sales'
        assert sales_row.score == 80.0

    def test_summarize_over_tagscores_automatic_filters(self):
        """ Test a dataset that has multiple rows per user """
        self.shelf = tagscores_shelf
        recipe = self.recipe().metrics('score').dimensions(
            'department', 'username'
        ).automatic_filters({
            'tag': 'musician'
        }).summarize_over('username')

        assert recipe.to_sql() == """SELECT summarize.department,
       sum(summarize.score) AS score
FROM
  (SELECT tagscores.department AS department,
          tagscores.username AS username,
          avg(tagscores.score) AS score
   FROM tagscores
   WHERE tagscores.tag = 'musician'
   GROUP BY tagscores.department,
            tagscores.username) AS summarize
GROUP BY summarize.department"""
        row = recipe.one()
        assert row.department == 'ops'
        assert row.score == 90

    def test_summarize_over_tagscores_test_cnt(self):
        """ Test a dataset that has multiple rows per user """
        self.shelf = tagscores_shelf
        recipe = self.recipe().metrics('test_cnt').dimensions(
            'department', 'username'
        ).summarize_over('username')

        assert recipe.to_sql() == """SELECT summarize.department,
       sum(summarize.test_cnt) AS test_cnt
FROM
  (SELECT tagscores.department AS department,
          tagscores.username AS username,
          count(DISTINCT tagscores.testid) AS test_cnt
   FROM tagscores
   GROUP BY tagscores.department,
            tagscores.username) AS summarize
GROUP BY summarize.department"""
        ops_row, sales_row = recipe.all()
        assert ops_row.department == 'ops'
        assert ops_row.test_cnt == 5
        assert sales_row.department == 'sales'
        assert sales_row.test_cnt == 1


class TestCompareRecipeExtension(object):

    def setup(self):
        # create a Session
        self.session = oven.Session()

        self.shelf = copy(census_shelf)
        self.extension_classes = [CompareRecipe]

    def recipe(self):
        return Recipe(
            shelf=self.shelf,
            session=self.session,
            extension_classes=self.extension_classes
        )

    def test_compare(self):
        """ A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics('pop2000').dimensions('sex').order_by('sex')
        r = r.compare(
            self.recipe().metrics('pop2000').dimensions('sex')
            .filters(Census.state == 'Vermont')
        )

        assert len(r.all()) == 2
        assert r.to_sql() == """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       avg(anon_1.pop2000) AS pop2000_compare
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
        assert rowwomen.pop2000 == 3234901
        assert rowwomen.pop2000_compare == 310948
        assert rowmen.sex == 'M'
        assert rowmen.pop2000 == 3059809
        assert rowmen.pop2000_compare == 298532

    def test_compare_custom_aggregation(self):
        """ A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """
        r = self.recipe().metrics('pop2000').dimensions('sex').order_by('sex')
        r = r.compare(
            self.recipe().metrics('pop2000_sum').dimensions('sex')
            .filters(Census.state == 'Vermont')
        )

        assert len(r.all()) == 2
        assert r.to_sql() == """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       sum(anon_1.pop2000_sum) AS pop2000_sum_compare
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000_sum
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY census.sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex"""
        rowwomen, rowmen = r.all()[0], r.all()[1]
        # We should get the lookup values
        assert rowwomen.sex == 'F'
        assert rowwomen.pop2000 == 3234901
        assert rowwomen.pop2000_sum_compare == 53483056
        assert rowmen.sex == 'M'
        assert rowmen.pop2000 == 3059809
        assert rowmen.pop2000_sum_compare == 51347504

    def test_compare_suffix(self):
        """ Test that the proper suffix gets added to the comparison metrics
        """

        r = self.recipe().metrics('pop2000').dimensions('sex').order_by('sex')
        r = r.compare(
            self.recipe().metrics('pop2000').dimensions('sex')
            .filters(Census.state == 'Vermont'),
            suffix='_x'
        )

        assert len(r.all()) == 2
        assert r.to_sql() == """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       avg(anon_1.pop2000) AS pop2000_x
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
        assert rowwomen.pop2000 == 3234901
        assert rowwomen.pop2000_x == 310948
        assert not hasattr(rowwomen, 'pop2000_compare')
        assert rowmen.sex == 'M'
        assert rowmen.pop2000 == 3059809
        assert rowmen.pop2000_x == 298532
        assert not hasattr(rowmen, 'pop2000_compare')

    def test_multiple_compares(self):
        """ Test that we can do multiple comparisons
        """

        r = self.recipe().metrics('pop2000').dimensions('sex',
                                                        'state').order_by(
                                                            'sex', 'state'
                                                        )
        r = r.compare(
            self.recipe().metrics('pop2000').dimensions('sex')
            .filters(Census.state == 'Vermont'),
            suffix='_vermont'
        )
        r = r.compare(self.recipe().metrics('pop2000'), suffix='_total')

        assert len(r.all()) == 4
        assert r.to_sql() == """SELECT census.sex AS sex,
       census.state AS state,
       sum(census.pop2000) AS pop2000,
       avg(anon_1.pop2000) AS pop2000_vermont,
       avg(anon_2.pop2000) AS pop2000_total
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

        tennessee_women, vermont_women = r.all()[0], r.all()[1]
        assert tennessee_women.sex == 'F'
        assert tennessee_women.pop2000 == 2923953
        assert tennessee_women.pop2000_vermont == 310948
        assert tennessee_women.pop2000_total == 6294710
        assert not hasattr(tennessee_women, 'pop2000_compare')
        assert vermont_women.sex == 'F'
        assert vermont_women.pop2000 == 310948
        assert vermont_women.pop2000_vermont == 310948
        assert vermont_women.pop2000_total == 6294710
        assert not hasattr(vermont_women, 'pop2000_compare')

    def test_mismatched_dimensions_raises(self):
        """ Dimensions in the comparison recipe must be a subset of the
        dimensions in the base recipe """
        r = self.recipe().metrics('pop2000').dimensions('sex').order_by('sex')
        r = r.compare(
            self.recipe().metrics('pop2000').dimensions('state')
            .filters(Census.state == 'Vermont'),
            suffix='_x'
        )

        with pytest.raises(BadRecipe):
            r.all()


class TestBlendRecipeExtension(object):

    def setup(self):
        # create a Session
        self.session = oven.Session()

        self.shelf = copy(census_shelf)
        self.extension_classes = [BlendRecipe]

    def recipe(self):
        return Recipe(
            shelf=self.shelf,
            session=self.session,
            extension_classes=self.extension_classes
        )

    def test_self_blend(self):
        """ A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics('pop2000').dimensions('sex').order_by('sex')

        blend_recipe = self.recipe() \
            .metrics('pop2008') \
            .dimensions('sex') \
            .filters(Census.sex == 'F')
        r = r.full_blend(blend_recipe, join_base='sex', join_blend='sex')

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
        assert rowwomen.pop2000 == 3234901
        assert rowwomen.pop2008 == 3499762
        assert rowmen.sex == 'M'
        assert rowmen.pop2000 == 3059809
        assert rowmen.pop2008 is None

    def test_blend(self):
        """ A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics('pop2000').dimensions('state'
                                                       ).order_by('state')

        blend_recipe = self.recipe().shelf(statefact_shelf) \
            .dimensions('state', 'abbreviation')
        r = r.blend(blend_recipe, join_base='state', join_blend='state')

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

        assert len(r.all()) == 2
        tennesseerow, vermontrow = r.all()[0], r.all()[1]
        assert tennesseerow.state == 'Tennessee'
        assert tennesseerow.state_id == 'Tennessee'
        assert tennesseerow.abbreviation == 'TN'
        assert tennesseerow.abbreviation_id == 'TN'
        assert tennesseerow.pop2000 == 5685230
        assert vermontrow.state == 'Vermont'
        assert vermontrow.state_id == 'Vermont'
        assert vermontrow.abbreviation == 'VT'
        assert vermontrow.abbreviation_id == 'VT'
        assert vermontrow.pop2000 == 609480
