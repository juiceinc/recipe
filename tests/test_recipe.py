from sqlalchemy import Table
from sqlalchemy import func

import recipe
from recipe import Dimension
from recipe import Having
from recipe import Metric
from recipe import Recipe
from recipe import Shelf
from .test_base import *


def test_main():
    assert recipe  # use your library here


class TestRecipeIngredients(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last),
            'age': Metric(func.sum(MyTable.age))
        })

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_dimension(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""
        assert recipe.all()[0].first == 'hi'
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

    def test_dimension2(self):
        recipe = self.recipe().metrics('age').dimensions('last').order_by(
            'last')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.last AS last
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_filter(self):
        recipe = self.recipe().metrics('age').dimensions(
            'last').filters(MyTable.age > 2).order_by('last')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.last AS last
FROM foo
WHERE foo.age > 2
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_having(self):
        hv = Having(func.sum(MyTable.age) < 10)
        recipe = self.recipe().metrics('age').dimensions(
            'last').filters(MyTable.age > 2).filters(hv).order_by('last')
        assert recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.last AS last
FROM foo
WHERE foo.age > 2
GROUP BY foo.last
HAVING sum(foo.age) < 10
ORDER BY foo.last"""



