import pytest

import recipe
from recipe import BadRecipe
from recipe import Having
from recipe import Recipe
from .test_base import *


def test_main():
    assert recipe  # use your library here


class TestRecipeIngredients(object):
    def setup(self):
        # create a Session
        self.session = Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

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
        assert recipe.dataset.json == '''[{"first": "hi", "age": 15, "first_id": "hi"}]'''

        # Line delimiter is \r\n
        assert recipe.dataset.csv == '''first,age,first_id\r
hi,15,hi\r
'''
        # Line delimiter is \r\n
        assert recipe.dataset.tsv == '''first\tage\tfirst_id\r
hi\t15\thi\r
'''

    def test_dimension2(self):
        recipe = self.recipe().metrics('age').dimensions('last').order_by(
            'last')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_filter(self):
        recipe = self.recipe().metrics('age').dimensions(
            'last').filters(MyTable.age > 2).order_by('last')
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
        recipe = self.recipe().metrics('age').dimensions(
            'last').filters(MyTable.age > 2).filters(hv).order_by('last')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 2
GROUP BY foo.last
HAVING sum(foo.age) < 10
ORDER BY foo.last"""

    def test_wtdavg(self):
        recipe = self.recipe().shelf(census_shelf) \
            .metrics('avgage').dimensions('state').order_by('-avgage')

        assert recipe.to_sql() == """SELECT census.state AS state,
       CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS avgage
FROM census
GROUP BY census.state
ORDER BY CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) DESC"""

        assert recipe.dataset.csv.replace('\r\n', '\n') == \
               """state,avgage,state_id
Florida,39.08283934000634,Florida
West Virginia,38.555058651148165,West Virginia
Maine,38.10118393261269,Maine
Pennsylvania,38.03856695544053,Pennsylvania
Rhode Island,37.20343773873182,Rhode Island
Connecticut,37.19867141455273,Connecticut
Iowa,37.078035709038765,Iowa
Vermont,37.0597968760254,Vermont
Montana,36.96749490527079,Montana
Massachusetts,36.93174172613041,Massachusetts
Hawaii,36.90632264720522,Hawaii
North Dakota,36.88621273600459,North Dakota
New Jersey,36.74068847555404,New Jersey
Arkansas,36.63745110262778,Arkansas
Oregon,36.496811702761605,Oregon
Missouri,36.44189917654348,Missouri
New Hampshire,36.43186070925119,New Hampshire
Ohio,36.4075566193844,Ohio
District of Columbia,36.40244110663024,District of Columbia
New York,36.345351106832155,New York
Delaware,36.330523552313004,Delaware
Alabama,36.27787892421841,Alabama
Wisconsin,36.25168689162029,Wisconsin
Tennessee,36.24667550829078,Tennessee
Kentucky,36.24473356470787,Kentucky
South Dakota,36.222866380141156,South Dakota
Oklahoma,36.11044391065816,Oklahoma
Nebraska,36.002538356871376,Nebraska
North Carolina,35.972044903724104,North Carolina
South Carolina,35.9403002120777,South Carolina
Wyoming,35.887778992364346,Wyoming
Virginia,35.83989223366432,Virginia
Kansas,35.80313080055561,Kansas
Maryland,35.78229569405914,Maryland
Michigan,35.73387612562981,Michigan
Indiana,35.65514855002191,Indiana
Minnesota,35.58193774119775,Minnesota
Washington,35.5077668783521,Washington
Illinois,35.37632181406334,Illinois
Arizona,35.37065466080318,Arizona
Nevada,35.2824568018656,Nevada
New Mexico,34.973244002951866,New Mexico
Mississippi,34.962652285245944,Mississippi
Louisiana,34.829653247822385,Louisiana
Colorado,34.5386073584527,Colorado
Idaho,34.33450207020143,Idaho
California,34.17872597484759,California
Georgia,34.0607146042724,Georgia
Texas,33.48920934903636,Texas
Alaska,31.947384766048568,Alaska
Utah,30.63622231900565,Utah
"""


class TestStats(object):
    def setup(self):
        # create a Session
        self.session = Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_stats(self):
        recipe = self.recipe().metrics('age').dimensions(
            'last')

        assert recipe.stats.ready == False
        with pytest.raises(BadRecipe):
            assert recipe.stats.rows == 5

        recipe.all()

        # Stats are ready after the recipe is run
        assert recipe.stats.ready == True
        assert recipe.stats.rows == 2


class TestCacheContext(object):
    def setup(self):
        # create a Session
        self.session = Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_cachec_context(self):
        recipe = self.recipe().metrics('age').dimensions(
            'last')
        recipe.cache_context = 'foo'

        assert len(recipe.all()) == 2
        assert recipe._cauldron['last'].cache_context == 'foo'
