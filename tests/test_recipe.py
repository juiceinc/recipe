import pytest
from sqlalchemy import func
from tests.test_base import MyTable, census_shelf, mytable_shelf, oven

from recipe import BadRecipe, Having, Recipe


class TestRecipeIngredients(object):

    def setup(self):
        # create a Session
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self, **kwargs):
        return Recipe(shelf=self.shelf, session=self.session, **kwargs)

    def test_dimension(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""
        assert recipe.all()[0].first == 'hi'
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

    def test_dataset(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        assert recipe.dataset.json == '[{"first": "hi", "age": 15, ' \
                                      '"first_id": "hi"}]'

        # Line delimiter is \r\n
        assert recipe.dataset.csv == '''first,age,first_id\r
hi,15,hi\r
'''
        # Line delimiter is \r\n
        assert recipe.dataset.tsv == '''first\tage\tfirst_id\r
hi\t15\thi\r
'''

    def test_dimension2(self):
        recipe = self.recipe().metrics('age').dimensions('last'
                                                        ).order_by('last')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_recipe_init(self):
        recipe = self.recipe(
            metrics=('age',), dimensions=('last',)
        ).order_by('last')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_filter(self):
        recipe = self.recipe().metrics('age').dimensions('last').filters(
            MyTable.age > 2
        ).order_by('last')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 2
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_having(self):
        hv = Having(func.sum(MyTable.age) < 10)
        recipe = self.recipe().metrics('age').dimensions('last').filters(
            MyTable.age > 2
        ).filters(hv).order_by('last')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 2
GROUP BY foo.last
HAVING sum(foo.age) < 10
ORDER BY foo.last"""

        assert recipe.dataset.csv.replace('\r\n', '\n') == \
        """last,age,last_id
there,5,there
"""


class TestStats(object):

    def setup(self):
        # create a Session
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_stats(self):
        recipe = self.recipe().metrics('age').dimensions('last')

        assert recipe.stats.ready is False
        with pytest.raises(BadRecipe):
            assert recipe.stats.rows == 5

        recipe.all()

        # Stats are ready after the recipe is run
        assert recipe.stats.ready is True
        assert recipe.stats.rows == 2
        assert recipe.stats.dbtime < 1.0
        assert recipe.stats.from_cache is False


class TestCacheContext(object):

    def setup(self):
        # create a Session
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_cachec_context(self):
        recipe = self.recipe().metrics('age').dimensions('last')
        recipe.cache_context = 'foo'

        assert len(recipe.all()) == 2
        assert recipe._cauldron['last'].cache_context == 'foo'
