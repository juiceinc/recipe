from copy import copy

import pytest
from sqlalchemy import func, join
from tests.test_base import (
    Census,
    MyTable,
    Scores,
    StateFact,
    census_shelf,
    mytable_shelf,
    oven,
)

from recipe import BadRecipe, Dimension, Filter, Having, Metric, Recipe, Shelf


class TestRecipeIngredients(object):
    def setup(self):
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self, **kwargs):
        return Recipe(shelf=self.shelf, session=self.session, **kwargs)

    def test_dimension(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY first"""
        )
        assert recipe.all()[0].first == "hi"
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

    def test_idvaluedimension(self):
        recipe = self.recipe().metrics("age").dimensions("firstlast")
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS firstlast_id,
       foo.last AS firstlast,
       sum(foo.age) AS age
FROM foo
GROUP BY firstlast_id,
         firstlast"""
        )
        assert recipe.all()[0].firstlast == "fred"
        assert recipe.all()[0].firstlast_id == "hi"
        assert recipe.all()[0].age == 10
        assert recipe.all()[1].firstlast == "there"
        assert recipe.all()[1].firstlast_id == "hi"
        assert recipe.all()[1].age == 5
        assert recipe.stats.rows == 2

    def test_multirole_dimension(self):
        """Create a dimension with extra roles and lookup"""
        d = Dimension(
            MyTable.last,
            id_expression=MyTable.first,
            age_expression=MyTable.age,
            id="d",
        )
        recipe = self.recipe().metrics("age").dimensions(d)
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS d_id,
       foo.last AS d,
       foo.age AS d_age,
       sum(foo.age) AS age
FROM foo
GROUP BY d_id,
         d,
         d_age"""
        )
        assert recipe.all()[0].d == "fred"
        assert recipe.all()[0].d_id == "hi"
        assert recipe.all()[0].d_age == 10
        assert recipe.all()[0].age == 10
        assert recipe.all()[1].d == "there"
        assert recipe.all()[1].d_id == "hi"
        assert recipe.all()[1].d_age == 5
        assert recipe.all()[1].age == 5
        assert recipe.stats.rows == 2

    def test_multirole_dimension_with_lookup(self):
        """Create a dimension with extra roles and lookup"""
        d = Dimension(
            MyTable.last,
            id_expression=MyTable.first,
            age_expression=MyTable.age,
            id="d",
            lookup={},
            lookup_default="DEFAULT",
        )
        recipe = self.recipe().metrics("age").dimensions(d)

        assert (
            recipe.to_sql()
            == """SELECT foo.first AS d_id,
       foo.last AS d_raw,
       foo.age AS d_age,
       sum(foo.age) AS age
FROM foo
GROUP BY d_id,
         d_raw,
         d_age"""
        )

        assert recipe.all()[0].d_raw == "fred"
        assert recipe.all()[0].d == "DEFAULT"
        assert recipe.all()[0].d_id == "hi"
        assert recipe.all()[0].d_age == 10
        assert recipe.all()[0].age == 10
        assert recipe.all()[1].d_raw == "there"
        assert recipe.all()[1].d == "DEFAULT"
        assert recipe.all()[1].d_id == "hi"
        assert recipe.all()[1].d_age == 5
        assert recipe.all()[1].age == 5
        assert recipe.stats.rows == 2

    def test_offset(self):
        recipe = self.recipe().metrics("age").dimensions("first").offset(1)
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY first
LIMIT ?
OFFSET 1"""
        )
        assert len(recipe.all()) == 0
        assert recipe.stats.rows == 0
        assert recipe.one() == []
        assert recipe.dataset.csv == "\r\n"

    def test_is_postgres(self):
        recipe = self.recipe().metrics("age")
        assert recipe._is_postgres() is False

    def test_use_cache(self):
        recipe = self.recipe().metrics("age")
        assert recipe._use_cache is True

        recipe = self.recipe().metrics("age").use_cache(False)
        assert recipe._use_cache is False

        with pytest.raises(AssertionError):
            self.recipe().metrics("age").use_cache("potatoe")

    def test_cache_region(self):
        recipe = self.recipe().metrics("age")
        assert recipe._cache_region == "default"

        recipe = self.recipe().metrics("age").cache_region("foo")
        assert recipe._cache_region == "foo"

        with pytest.raises(AssertionError):
            self.recipe().metrics("age").cache_region(22)

    def test_cache_prefix(self):
        recipe = self.recipe().metrics("age")
        assert recipe._cache_prefix == "default"

        recipe = self.recipe().metrics("age").cache_prefix("foo")
        assert recipe._cache_prefix == "foo"

        with pytest.raises(AssertionError):
            self.recipe().metrics("age").cache_prefix(22)

    def test_dataset(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        assert (
            recipe.dataset.json == '[{"first": "hi", "age": 15, ' '"first_id": "hi"}]'
        )

        # Line delimiter is \r\n
        assert (
            recipe.dataset.csv
            == """first,age,first_id\r
hi,15,hi\r
"""
        )
        # Line delimiter is \r\n
        assert (
            recipe.dataset.tsv
            == """first\tage\tfirst_id\r
hi\t15\thi\r
"""
        )

    def test_session(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY first"""
        )
        assert recipe.all()[0].first == "hi"
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

        sess = oven.Session()
        recipe.reset()
        recipe.session(sess)

    def test_shelf(self):
        recipe = self.recipe().metrics("age").dimensions("first")
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
        recipe = self.recipe().metrics("age").dimensions("last").order_by("last")
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY last"""
        )
        assert recipe.all()[0].last == "fred"
        assert recipe.all()[0].last_id == "fred"
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

    def test_order_bys(self):
        recipe = self.recipe().metrics("age").dimensions("last").order_by("last")
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY last"""
        )
        assert recipe.all()[0].last == "fred"
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

        recipe = self.recipe().metrics("age").dimensions("last").order_by("age")
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY age"""
        )
        assert recipe.all()[0].last == "there"
        assert recipe.all()[0].age == 5
        assert recipe.stats.rows == 2

        recipe = self.recipe().metrics("age").dimensions("last").order_by("-age")
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY age DESC"""
        )
        assert recipe.all()[0].last == "fred"
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2

        # Idvalue dimension
        recipe = (
            self.recipe().metrics("age").dimensions("firstlast").order_by("firstlast")
        )
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS firstlast_id,
       foo.last AS firstlast,
       sum(foo.age) AS age
FROM foo
GROUP BY firstlast_id,
         firstlast
ORDER BY firstlast,
         firstlast_id"""
        )
        recipe = (
            self.recipe().metrics("age").dimensions("firstlast").order_by("-firstlast")
        )
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS firstlast_id,
       foo.last AS firstlast,
       sum(foo.age) AS age
FROM foo
GROUP BY firstlast_id,
         firstlast
ORDER BY firstlast DESC,
         firstlast_id DESC"""
        )

        # Dimensions can define their own ordering
        d = Dimension(
            MyTable.last,
            id="d",
            id_expression=MyTable.first,
            order_by_expression=func.upper(MyTable.last),
        )
        recipe = self.recipe().metrics("age").dimensions(d).order_by(d)
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS d_id,
       foo.last AS d,
       upper(foo.last) AS d_order_by,
       sum(foo.age) AS age
FROM foo
GROUP BY d_id,
         d,
         d_order_by
ORDER BY d_order_by,
         d,
         d_id"""
        )

    def test_recipe_init(self):
        """Test that all options can be passed in the init"""
        recipe = self.recipe(metrics=("age",), dimensions=("last",)).order_by("last")
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY last"""
        )
        assert recipe.all()[0].last == "fred"
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2
        recipe = self.recipe(
            metrics=["age"],
            dimensions=["first"],
            filters=[MyTable.age > 4],
            order_by=["first"],
        )
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 4
GROUP BY first
ORDER BY first"""
        )
        assert recipe.all()[0].first == "hi"
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

    def test_from_config(self):
        shelf = copy(mytable_shelf)
        shelf["ageover4"] = Filter(MyTable.age > 4)
        config = {
            "dimensions": ["first", "last"],
            "metrics": ["age"],
            "filters": ["ageover4"],
        }
        recipe = Recipe.from_config(shelf, config).session(self.session)
        assert (
            recipe.to_sql()
            == """\
SELECT foo.first AS first,
       foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 4
GROUP BY first,
         last"""
        )

    def test_order_bys_not_matching_ingredients(self):
        """If an order_by is not found in dimensions+metrics, we ignore it"""
        recipe = self.recipe().metrics("age").dimensions("first").order_by("last")
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY first"""
        )
        assert recipe.all()[0].first == "hi"
        assert recipe.all()[0].age == 15
        assert recipe.stats.rows == 1

    def test_from_config_filter_object(self):
        config = {
            "dimensions": ["last"],
            "metrics": ["age"],
            "filters": [{"field": "age", "gt": 13}],
        }

        shelf = copy(mytable_shelf)
        # The shelf must have an accurate `select_from` in order to allow
        # passing in full ingredient structures, instead of just names. That's
        # because we need to be able to map a field name to an actual column.
        shelf.Meta.select_from = MyTable
        recipe = Recipe.from_config(shelf, config).session(self.session)
        assert (
            recipe.to_sql()
            == """\
SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 13
GROUP BY last"""
        )

    def test_from_config_extra_kwargs(self):
        config = {"dimensions": ["last"], "metrics": ["age"]}
        recipe = Recipe.from_config(self.shelf, config, order_by=["last"]).session(
            self.session
        )
        assert (
            recipe.to_sql()
            == """\
SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY last"""
        )

    def test_recipe_empty(self):
        recipe = self.recipe()
        with pytest.raises(BadRecipe):
            recipe.all()

    def test_recipe_multi_tables(self):
        dim = Dimension(Scores.username)
        recipe = self.recipe().dimensions("last", dim).metrics("age")
        with pytest.raises(BadRecipe):
            recipe.all()

    def test_recipe_table(self):
        recipe = self.recipe().dimensions("last").metrics("age")
        assert recipe._table() == MyTable

    def test_recipe_as_table(self):
        recipe = self.recipe().dimensions("last").metrics("age")
        tbl = recipe.as_table()
        assert tbl.name == recipe._id

        tbl = recipe.as_table(name="foo")
        assert tbl.name == "foo"

    def test_filter(self):
        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("last")
            .filters(MyTable.age > 2)
            .order_by("last")
        )
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 2
GROUP BY last
ORDER BY last"""
        )
        assert recipe.all()[0].last == "fred"
        assert recipe.all()[0].age == 10
        assert recipe.stats.rows == 2
        assert len(list(recipe.filter_ids)) == 1

    def test_having(self):
        hv = Having(func.sum(MyTable.age) < 10)
        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("last")
            .filters(MyTable.age > 2)
            .filters(hv)
            .order_by("last")
        )
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 2
GROUP BY last
HAVING sum(foo.age) < 10
ORDER BY last"""
        )

        assert (
            recipe.dataset.csv.replace("\r\n", "\n")
            == """last,age,last_id
there,5,there
"""
        )

    def test_condition(self):
        yaml = """
oldage:
    kind: Metric
    field:
        value: age
        condition:
            field: age
            gt: 60
"""
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("oldage")
        assert (
            " ".join(recipe.to_sql().split()) == "SELECT sum(CASE WHEN (foo.age > 60) "
            "THEN foo.age END) AS oldage FROM foo"
        )

    def test_percentile(self):
        yaml = """
p75:
    kind: Metric
    field:
        value: pop2000
        aggregation: percentile75
"""
        shelf = Shelf.from_validated_yaml(yaml, Census)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("p75")
        assert (
            recipe.to_sql()
            == """SELECT percentile_cont(0.75) WITHIN GROUP (
                                           ORDER BY census.pop2000) AS p75
FROM census"""
        )

    def test_percentile_with_dimension(self):
        """ While this query doesn't run in sqlite, it has been tested in
        redshift and bigquery """
        yaml = """
state:
    kind: Dimension
    field: state
p75:
    kind: Metric
    field:
        value: pop2000
        aggregation: percentile75
"""
        shelf = Shelf.from_validated_yaml(yaml, Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session).metrics("p75").dimensions("state")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state,
       percentile_cont(0.75) WITHIN GROUP (
                                           ORDER BY census.pop2000) AS p75
FROM census
GROUP BY state"""
        )

    def test_cast(self):
        yaml = """
intage:
    kind: Metric
    field:
        value: age
        as: integer
"""
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("intage")
        assert (
            " ".join(recipe.to_sql().split())
            == "SELECT CAST(sum(foo.age) AS INTEGER) AS intage FROM foo"
        )

    def test_coalesce(self):
        yaml = """
defaultage:
    kind: Metric
    field:
        value: age
        default: 0.1
"""
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("defaultage")
        assert (
            " ".join(recipe.to_sql().split())
            == "SELECT coalesce(sum(foo.age), 0.1) AS defaultage FROM foo"
        )

    def test_field_with_add_float(self):
        yaml = """
addage:
    kind: Metric
    field: 'age + 1.24'
"""
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("addage")
        assert (
            " ".join(recipe.to_sql().split())
            == "SELECT sum(foo.age + 1.24) AS addage FROM foo"
        )

    def test_compound_and_condition(self):
        yaml = """
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
"""
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("oldageandcoolname")
        assert (
            " ".join(recipe.to_sql().split())
            == "SELECT sum(CASE WHEN (foo.age > 60 AND foo.first = 'radix') "
            "THEN foo.age END) AS oldageandcoolname FROM foo"
        )

    def test_compound_or_condition(self):
        yaml = """
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
"""
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("oldageorcoolname")
        assert (
            " ".join(recipe.to_sql().split())
            == "SELECT sum(CASE WHEN (foo.age > 60 OR foo.first = 'radix') "
            "THEN foo.age END) AS oldageorcoolname FROM foo"
        )

    def test_divide_by(self):
        yaml = """
divider:
    kind: Metric
    field: age
    divide_by: age
"""
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("divider")
        assert (
            " ".join(recipe.to_sql().split()) == "SELECT CAST(sum(foo.age) AS FLOAT) / "
            "(coalesce(CAST(sum(foo.age) AS FLOAT), 0.0) + 1e-09) "
            "AS divider FROM foo"
        )

    def test_wtd_avg(self):
        yaml = """
divider:
    kind: Metric
    field: age*age
    divide_by: age
"""
        shelf = Shelf.from_validated_yaml(yaml, MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("divider")
        assert (
            " ".join(recipe.to_sql().split())
            == "SELECT CAST(sum(foo.age * foo.age) AS FLOAT) / "
            "(coalesce(CAST(sum(foo.age) AS FLOAT), 0.0) + 1e-09) "
            "AS divider FROM foo"
        )

    def test_count(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        assert recipe.total_count() == 1
        recipe = self.recipe().metrics("age").dimensions("last")
        assert recipe.total_count() == 2


class TestStats(object):
    def setup(self):
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_stats(self):
        recipe = self.recipe().metrics("age").dimensions("last")
        recipe.all()

        # Stats are ready after the recipe is run
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
        recipe = self.recipe().metrics("age").dimensions("last")
        from recipe import Metric, Dimension

        subq = recipe.subquery(name="anon")
        nested_shelf = Shelf(
            {"age": Metric(func.sum(subq.c.age)), "last": Dimension(subq.c.last)}
        )

        r = (
            Recipe(shelf=nested_shelf, session=self.session)
            .dimensions("last")
            .metrics("age")
        )
        assert (
            r.to_sql()
            == """SELECT anon.last AS last,
       sum(anon.age) AS age
FROM
  (SELECT foo.last AS last,
          sum(foo.age) AS age
   FROM foo
   GROUP BY last) AS anon
GROUP BY last"""
        )
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

        shelf = Shelf(
            {
                "region": Dimension(StateFact.census_region_name),
                "pop": Metric(func.sum(Census.pop2000)),
            }
        )

        r = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("region")
            .metrics("pop")
            .select_from(j)
        )

        assert (
            r.to_sql()
            == """SELECT state_fact.census_region_name AS region,
       sum(census.pop2000) AS pop
FROM census
JOIN state_fact ON census.state = state_fact.name
GROUP BY region"""
        )
        assert (
            r.dataset.tsv
            == """region\tpop\tregion_id\r
Northeast\t609480\tNortheast\r
South\t5685230\tSouth\r
"""
        )
        assert len(r.all()) == 2


class TestShelfSelectFrom(object):
    def setup(self):
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_recipe_with_select_from(self):
        from recipe import Dimension, Metric

        shelf = Shelf(
            {
                "region": Dimension(StateFact.census_region_name),
                "pop": Metric(func.sum(Census.pop2000)),
            },
            select_from=join(Census, StateFact, Census.state == StateFact.name),
        )

        assert shelf.Meta.select_from is not None

        r = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("region")
            .metrics("pop")
        )

        assert r._select_from is not None

        assert (
            r.to_sql()
            == """SELECT state_fact.census_region_name AS region,
       sum(census.pop2000) AS pop
FROM census
JOIN state_fact ON census.state = state_fact.name
GROUP BY region"""
        )
        assert (
            r.dataset.tsv
            == """region\tpop\tregion_id\r
Northeast\t609480\tNortheast\r
South\t5685230\tSouth\r
"""
        )
        assert len(r.all()) == 2

    def test_recipe_as_selectable_from_validated_yaml(self):
        """ A recipe can be used as a selectable for a shelf
        created from yaml """
        recipe = (
            Recipe(shelf=census_shelf, session=self.session)
            .metrics("pop2000")
            .dimensions("state")
        )

        yaml = """
        pop:
            kind: Metric
            field:
                value: pop2000
                aggregation: avg
        """

        recipe_shelf = Shelf.from_validated_yaml(yaml, recipe)

        r = Recipe(shelf=recipe_shelf, session=self.session).metrics("pop")
        assert (
            r.to_sql()
            == """SELECT avg(anon_1.pop2000) AS pop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000
   FROM census
   GROUP BY state) AS anon_1"""
        )
        assert r.dataset.tsv == """pop\r\n3147355.0\r\n"""

    def test_recipe_as_selectable_from_yaml(self):
        """ A recipe can be used as a selectable for a shelf
        created from yaml """
        recipe = (
            Recipe(shelf=census_shelf, session=self.session)
            .metrics("pop2000")
            .dimensions("state")
        )

        yaml = """
        pop:
            kind: Metric
            field:
                value: pop2000
                aggregation: avg
        """

        recipe_shelf = Shelf.from_yaml(yaml, recipe)

        r = Recipe(shelf=recipe_shelf, session=self.session).metrics("pop")
        assert (
            r.to_sql()
            == """SELECT avg(anon_1.pop2000) AS pop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000
   FROM census
   GROUP BY state) AS anon_1"""
        )
        assert r.dataset.tsv == """pop\r\n3147355.0\r\n"""

    def test_recipe_as_subquery(self):
        """ Use a recipe subquery as a source for generating a new shelf."""

        recipe = (
            Recipe(shelf=census_shelf, session=self.session)
            .metrics("pop2000")
            .dimensions("state")
        )

        # Another approach is to use
        subq = recipe.subquery()
        recipe_subquery_shelf = Shelf({"pop": Metric(func.avg(subq.c.pop2000))})
        r2 = Recipe(shelf=recipe_subquery_shelf, session=self.session).metrics("pop")
        assert (
            r2.to_sql()
            == """SELECT avg(anon_1.pop2000) AS pop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000
   FROM census
   GROUP BY state) AS anon_1"""
        )
        assert r2.dataset.tsv == """pop\r\n3147355.0\r\n"""


class TestCacheContext(object):
    def setup(self):
        self.session = oven.Session()
        self.shelf = mytable_shelf

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_cachec_context(self):
        recipe = self.recipe().metrics("age").dimensions("last")
        recipe.cache_context = "foo"

        assert len(recipe.all()) == 2
        assert recipe._cauldron["last"].cache_context == "foo"
