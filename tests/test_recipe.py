from copy import copy

import pytest
from sqlalchemy import func, join
from tests.test_base import (
    Census, MyTable, Scores, StateFact, census_shelf, mytable_shelf, oven
)

from recipe import BadRecipe, Dimension, Filter, Having, Metric, Recipe, Shelf


class TestRecipeIngredients(object):

    def setup(self):
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

    def test_idvaluedimension(self):
        recipe = self.recipe().metrics('age').dimensions('firstlast')
        assert recipe.to_sql() == """SELECT foo.first AS firstlast_id,
       foo.last AS firstlast,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first,
         foo.last"""
        assert recipe.all()[0].firstlast == 'fred'
        assert recipe.all()[0].firstlast_id == 'hi'
        assert recipe.all()[0].age == 10
        assert recipe.all()[1].firstlast == 'there'
        assert recipe.all()[1].firstlast_id == 'hi'
        assert recipe.all()[1].age == 5
        assert recipe.stats.rows == 2

    def test_offset(self):
        recipe = self.recipe().metrics('age').dimensions('first').offset(1)
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first LIMIT ?
OFFSET 1"""
        assert len(recipe.all()) == 0
        assert recipe.stats.rows == 0
        assert recipe.one() == []
        assert recipe.dataset.csv == '\r\n'

    def test_is_postgres(self):
        recipe = self.recipe().metrics('age')
        assert recipe._is_postgres() is False

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

    def test_session(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.first"""
        assert recipe.all()[0].first == 'hi'
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1
        assert recipe.dirty is False

        sess = oven.Session()
        recipe.session(sess)
        assert recipe.dirty is True

    def test_shelf(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        assert len(recipe._shelf) == 4
        recipe.shelf(None)
        assert len(recipe._shelf) == 0
        recipe.shelf(self.shelf)
        assert len(recipe._shelf) == 4
        recipe.shelf({})
        assert len(recipe._shelf) == 0
        with pytest.raises(BadRecipe):
            recipe.shelf(52)

    def test_dimension2(self):
        recipe = self.recipe().metrics('age') \
            .dimensions('last') \
            .order_by('last')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].last_id == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_order_bys(self):
        recipe = self.recipe().metrics('age')\
            .dimensions('last').order_by('last')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

        recipe = self.recipe().metrics('age')\
            .dimensions('last').order_by('age')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY sum(foo.age)"""
        assert recipe.all()[0].last == 'there'
        assert recipe.all()[0].age == 5
        assert recipe.stats.rows == 2

        recipe = self.recipe().metrics('age')\
            .dimensions('last').order_by('-age')
        assert recipe.to_sql() == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY sum(foo.age) DESC"""
        assert recipe.all()[0].last == 'fred'
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_recipe_init(self):
        """Test that all options can be passed in the init"""
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
        recipe = self.recipe(
            metrics=['age'],
            dimensions=['first'],
            filters=[MyTable.age > 4],
            order_by=['first']
        )
        assert recipe.to_sql() == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 4
GROUP BY foo.first
ORDER BY foo.first"""
        assert recipe.all()[0].first == 'hi'
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

    def test_from_config(self):
        shelf = copy(mytable_shelf)
        shelf['ageover4'] = Filter(MyTable.age > 4)
        config = {
            'dimensions': ['first', 'last'],
            'metrics': ['age'],
            'filters': ['ageover4'],
        }
        recipe = Recipe.from_config(shelf, config).session(self.session)
        assert recipe.to_sql() == """\
SELECT foo.first AS first,
       foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 4
GROUP BY foo.first,
         foo.last"""

    def test_from_config_filter_object(self):
        config = {
            'dimensions': ['last'],
            'metrics': ['age'],
            'filters': [
                {'field': 'age', 'gt': 13},
            ]
        }

        shelf = copy(mytable_shelf)
        # The shelf must have an accurate `select_from` in order to allow
        # passing in full ingredient structures, instead of just names. That's
        # because we need to be able to map a field name to an actual column.
        shelf.Meta.select_from = MyTable
        recipe = Recipe.from_config(shelf, config).session(self.session)
        assert recipe.to_sql() == """\
SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 13
GROUP BY foo.last"""

    def test_from_config_extra_kwargs(self):
        config = {'dimensions': ['last'], 'metrics': ['age']}
        recipe = Recipe.from_config(
            self.shelf, config,
            order_by=['last'],
        ).session(self.session)
        assert recipe.to_sql() == """\
SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""

    def test_recipe_empty(self):
        recipe = self.recipe()
        with pytest.raises(BadRecipe):
            recipe.all()

    def test_recipe_multi_tables(self):
        dim = Dimension(Scores.username)
        recipe = self.recipe().dimensions('last', dim).metrics('age')
        with pytest.raises(BadRecipe):
            recipe.all()

    def test_recipe_table(self):
        recipe = self.recipe().dimensions('last').metrics('age')
        assert recipe._table() == MyTable

    def test_recipe_as_table(self):
        recipe = self.recipe().dimensions('last').metrics('age')
        tbl = recipe.as_table()
        assert tbl.name == recipe._id

        tbl = recipe.as_table(name='foo')
        assert tbl.name == 'foo'

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
        assert len(list(recipe.filter_ids)) == 1

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

    def test_condition(self):
        yaml = '''
oldage:
    kind: Metric
    field:
        value: age
        condition:
            field: age
            gt: 60
'''
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics('oldage')
        assert (
            ' '.join(recipe.to_sql().split()
                    ) == 'SELECT sum(CASE WHEN (foo.age > 60) '
            'THEN foo.age END) AS oldage FROM foo'
        )

    def test_compound_and_condition(self):
        yaml = '''
oldageandcoolname:
    kind: Metric
    field:
        value: age
        condition:
            and:
                - field: age
                  gt: 60
                - field: first
                  eq: radix
'''
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).metrics('oldageandcoolname')
        assert (
            ' '.join(
                recipe.to_sql().split()
            ) == "SELECT sum(CASE WHEN (foo.age > 60 AND foo.first = 'radix') "
            'THEN foo.age END) AS oldageandcoolname FROM foo'
        )

    def test_compound_or_condition(self):
        yaml = '''
oldageorcoolname:
    kind: Metric
    field:
        value: age
        condition:
            or:
                - field: age
                  gt: 60
                - field: first
                  eq: radix
'''
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(
            shelf=shelf, session=self.session
        ).metrics('oldageorcoolname')
        assert (
            ' '.join(
                recipe.to_sql().split()
            ) == "SELECT sum(CASE WHEN (foo.age > 60 OR foo.first = 'radix') "
            'THEN foo.age END) AS oldageorcoolname FROM foo'
        )

    def test_count(self):
        recipe = self.recipe().metrics('age').dimensions('first')
        assert recipe.total_count() == 1
        recipe = self.recipe().metrics('age').dimensions('last')
        assert recipe.total_count() == 2


class TestStats(object):

    def setup(self):
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
        assert recipe.stats.enchanttime < 1.0
        assert recipe.stats.from_cache is False


class TestNestedRecipe(object):

    def setup(self):
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_nested_recipe(self):
        recipe = self.recipe().metrics('age').dimensions('last')
        from recipe import Metric, Dimension

        subq = recipe.subquery(name='anon')
        nested_shelf = Shelf({
            'age': Metric(func.sum(subq.c.age)),
            'last': Dimension(subq.c.last)
        })

        r = Recipe(
            shelf=nested_shelf, session=self.session
        ).dimensions('last').metrics('age')
        assert r.to_sql() == '''SELECT anon.last AS last,
       sum(anon.age) AS age
FROM
  (SELECT foo.last AS last,
          sum(foo.age) AS age
   FROM foo
   GROUP BY foo.last) AS anon
GROUP BY anon.last'''
        assert len(r.all()) == 2


class TestSelectFrom(object):

    def setup(self):
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_recipe_with_select_from(self):
        j = join(Census, StateFact, Census.state == StateFact.name)

        from recipe import Dimension, Metric
        shelf = Shelf({
            'region': Dimension(StateFact.census_region_name),
            'pop': Metric(func.sum(Census.pop2000))
        })

        r = Recipe(shelf=shelf, session=self.session)\
            .dimensions('region').metrics('pop').select_from(j)

        assert r.to_sql() == """SELECT state_fact.census_region_name AS region,
       sum(census.pop2000) AS pop
FROM census
JOIN state_fact ON census.state = state_fact.name
GROUP BY state_fact.census_region_name"""
        assert r.dataset.tsv == '''region\tpop\tregion_id\r
Northeast\t609480\tNortheast\r
South\t5685230\tSouth\r
'''
        assert len(r.all()) == 2


class TestShelfSelectFrom(object):

    def setup(self):
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_recipe_with_select_from(self):
        from recipe import Dimension, Metric
        shelf = Shelf({
            'region': Dimension(StateFact.census_region_name),
            'pop': Metric(func.sum(Census.pop2000))
        },
                      select_from=join(
                          Census, StateFact, Census.state == StateFact.name
                      ))

        assert shelf.Meta.select_from is not None

        r = Recipe(shelf=shelf, session=self.session)\
            .dimensions('region').metrics('pop')

        assert r._select_from is not None

        assert r.to_sql() == """SELECT state_fact.census_region_name AS region,
       sum(census.pop2000) AS pop
FROM census
JOIN state_fact ON census.state = state_fact.name
GROUP BY state_fact.census_region_name"""
        assert r.dataset.tsv == '''region\tpop\tregion_id\r
Northeast\t609480\tNortheast\r
South\t5685230\tSouth\r
'''
        assert len(r.all()) == 2

    def test_recipe_as_selectable_from_validated_yaml(self):
        """ A recipe can be used as a selectable for a shelf
        created from yaml """
        recipe = Recipe(shelf=census_shelf, session=self.session) \
            .metrics('pop2000').dimensions('state')

        yaml = '''
        pop:
            kind: Metric
            field:
                value: pop2000
                aggregation: avg
        '''

        recipe_shelf = Shelf.from_validated_yaml(yaml, recipe)

        r = Recipe(shelf=recipe_shelf, session=self.session).metrics('pop')
        assert r.to_sql() == '''SELECT avg(anon_1.pop2000) AS pop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000
   FROM census
   GROUP BY census.state) AS anon_1'''
        assert r.dataset.tsv == '''pop\r\n3147355.0\r\n'''

    def test_recipe_as_selectable_from_yaml(self):
        """ A recipe can be used as a selectable for a shelf
        created from yaml """
        recipe = Recipe(shelf=census_shelf, session=self.session) \
            .metrics('pop2000').dimensions('state')

        yaml = '''
        pop:
            kind: Metric
            field:
                value: pop2000
                aggregation: avg
        '''

        recipe_shelf = Shelf.from_yaml(yaml, recipe)

        r = Recipe(shelf=recipe_shelf, session=self.session).metrics('pop')
        assert r.to_sql() == '''SELECT avg(anon_1.pop2000) AS pop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000
   FROM census
   GROUP BY census.state) AS anon_1'''
        assert r.dataset.tsv == '''pop\r\n3147355.0\r\n'''

    def test_recipe_as_subquery(self):
        """ Use a recipe subquery as a source for generating a new shelf."""

        recipe = Recipe(shelf=census_shelf, session=self.session)\
            .metrics('pop2000').dimensions('state')

        # Another approach is to use
        subq = recipe.subquery()
        recipe_subquery_shelf = Shelf({
            'pop': Metric(func.avg(subq.c.pop2000))
        })
        r2 = Recipe(
            shelf=recipe_subquery_shelf, session=self.session
        ).metrics('pop')
        assert r2.to_sql() == '''SELECT avg(anon_1.pop2000) AS pop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000
   FROM census
   GROUP BY census.state) AS anon_1'''
        assert r2.dataset.tsv == '''pop\r\n3147355.0\r\n'''


class TestCacheContext(object):

    def setup(self):
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_cachec_context(self):
        recipe = self.recipe().metrics('age').dimensions('last')
        recipe.cache_context = 'foo'

        assert len(recipe.all()) == 2
        assert recipe._cauldron['last'].cache_context == 'foo'
