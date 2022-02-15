from copy import copy

import pytest
from sqlalchemy import func, join
from tests.test_base import RecipeTestCase

from recipe import BadRecipe, Dimension, Filter, Having, Metric, Recipe, Shelf


class TestRecipeIngredients(RecipeTestCase):
    def test_dimension(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY first""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            first,age,first_id
            hi,15,hi
            """,
        )

    def test_raw_metrics_raw_dimensions(self):
        """Metric_ids and dimension_ids hold unique used metrics and dimensions.
        raw_metrics and raw_dimensions store ingredients as added."""

        # We call dimensions and metrics multiple times
        recipe = (
            self.recipe().metrics("age").dimensions("first", "first").metrics("age")
        )
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY first"""
        )
        self.assertRecipeCSV(
            recipe,
            """
            first,age,first_id
            hi,15,hi
            """,
        )
        self.assertEqual(recipe.metric_ids, ("age",))
        self.assertEqual(recipe.dimension_ids, ("first",))
        self.assertEqual(recipe.raw_dimensions, ("first", "first"))
        self.assertEqual(recipe.raw_metrics, ("age", "age"))

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
        self.assertRecipeCSV(
            recipe,
            """
            firstlast_id,firstlast,age,firstlast_id
            hi,fred,10,hi
            hi,there,5,hi
            """,
        )

    def test_multirole_dimension(self):
        """Create a dimension with extra roles and lookup"""
        d = Dimension(
            self.basic_table.c.last,
            id_expression=self.basic_table.c.first,
            age_expression=self.basic_table.c.age,
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
        self.assertRecipeCSV(
            recipe,
            """
            d_id,d,d_age,age,d_id
            hi,fred,10,10,hi
            hi,there,5,5,hi
            """,
        )

    def test_multirole_dimension_with_lookup(self):
        """Create a dimension with extra roles and lookup"""
        d = Dimension(
            self.basic_table.c.last,
            id_expression=self.basic_table.c.first,
            age_expression=self.basic_table.c.age,
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
        self.assertRecipeCSV(
            recipe,
            """
            d_id,d_raw,d_age,age,d,d_id
            hi,fred,10,10,DEFAULT,hi
            hi,there,5,5,DEFAULT,hi
            """,
        )

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
        self.assertEqual(len(recipe.all()), 0)
        self.assertEqual(recipe.stats.rows, 0)
        self.assertEqual(recipe.one(), [])

    def test_is_postgres(self):
        recipe = self.recipe().metrics("age")
        self.assertFalse(recipe._is_postgres())

    def test_use_cache(self):
        recipe = self.recipe().metrics("age")
        self.assertTrue(recipe._use_cache)

        recipe = self.recipe().metrics("age").use_cache(False)
        self.assertFalse(recipe._use_cache)

        with self.assertRaises(AssertionError):
            self.recipe().metrics("age").use_cache("potatoe")

    def test_cache_region(self):
        recipe = self.recipe().metrics("age")
        self.assertEqual(recipe._cache_region, "default")

        recipe = self.recipe().metrics("age").cache_region("foo")
        self.assertEqual(recipe._cache_region, "foo")

        with self.assertRaises(AssertionError):
            self.recipe().metrics("age").cache_region(22)

    def test_cache_prefix(self):
        recipe = self.recipe().metrics("age")
        self.assertEqual(recipe._cache_prefix, "default")

        recipe = self.recipe().metrics("age").cache_prefix("foo")
        self.assertEqual(recipe._cache_prefix, "foo")

        with self.assertRaises(AssertionError):
            self.recipe().metrics("age").cache_prefix(22)

    def test_dataset(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        assert recipe.dataset.json == '[{"first": "hi", "age": 15, "first_id": "hi"}]'

        # Line delimiter is \r\n
        self.assertEqual(recipe.dataset.csv, "first,age,first_id\r\nhi,15,hi\r\n")

    def test_session(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY first"""
        )
        self.assertEqual(recipe.all()[0].first, "hi")
        self.assertEqual(recipe.all()[0].age, 15)
        self.assertEqual(recipe.stats.rows, 1)

        sess = self.oven.Session()
        recipe.reset()
        recipe.session(sess)

    def test_shelf(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        self.assertEqual(len(recipe._shelf), 4)
        recipe.shelf(None)
        self.assertEqual(len(recipe._shelf), 0)
        recipe.shelf(self.shelf)
        self.assertEqual(len(recipe._shelf), 4)
        recipe.shelf({})
        self.assertEqual(len(recipe._shelf), 0)
        with self.assertRaises(BadRecipe):
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
        self.assertRecipeCSV(
            recipe,
            """
            last,age,last_id
            fred,10,fred
            there,5,there
            """,
        )

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
        self.assertRecipeCSV(
            recipe,
            """
            last,age,last_id
            fred,10,fred
            there,5,there
            """,
        )

        recipe = self.recipe().metrics("age").dimensions("last").order_by("age")
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY age"""
        )
        self.assertRecipeCSV(
            recipe,
            """
            last,age,last_id
            there,5,there
            fred,10,fred
            """,
        )

        recipe = self.recipe().metrics("age").dimensions("last").order_by("-age")
        assert (
            recipe.to_sql()
            == """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY age DESC"""
        )
        self.assertRecipeCSV(
            recipe,
            """
            last,age,last_id
            fred,10,fred
            there,5,there
            """,
        )

        # Idvalue dimension
        recipe = (
            self.recipe().metrics("age").dimensions("firstlast").order_by("firstlast")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS firstlast_id,
       foo.last AS firstlast,
       sum(foo.age) AS age
FROM foo
GROUP BY firstlast_id,
         firstlast
ORDER BY firstlast,
         firstlast_id""",
        )
        recipe = (
            self.recipe().metrics("age").dimensions("firstlast").order_by("-firstlast")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS firstlast_id,
       foo.last AS firstlast,
       sum(foo.age) AS age
FROM foo
GROUP BY firstlast_id,
         firstlast
ORDER BY firstlast DESC,
         firstlast_id DESC""",
        )

        # Dimensions can define their own ordering
        d = Dimension(
            self.basic_table.c.last,
            id="d",
            id_expression=self.basic_table.c.first,
            order_by_expression=func.upper(self.basic_table.c.last),
        )
        recipe = self.recipe().metrics("age").dimensions(d).order_by(d)
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS d_id,
       foo.last AS d,
       upper(foo.last) AS d_order_by,
       sum(foo.age) AS age
FROM foo
GROUP BY d_id,
         d,
         d_order_by
ORDER BY d_order_by,
         d,
         d_id""",
        )

    def test_recipe_init(self):
        """Test that all options can be passed in the init"""
        recipe = self.recipe(metrics=("age",), dimensions=("last",)).order_by("last")
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY last""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            last,age,last_id
            fred,10,fred
            there,5,there
            """,
        )

        recipe = self.recipe(
            metrics=["age"],
            dimensions=["first"],
            filters=[self.basic_table.c.age > 4],
            order_by=["first"],
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 4
GROUP BY first
ORDER BY first""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            first,age,first_id
            hi,15,hi
            """,
        )

    def test_from_config(self):
        shelf = copy(self.mytable_shelf)
        shelf["ageover4"] = Filter(self.basic_table.c.age > 4)
        config = {
            "dimensions": ["first", "last"],
            "metrics": ["age"],
            "filters": ["ageover4"],
        }
        recipe = self.recipe_from_config(config, shelf=shelf)
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS first,
       foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 4
GROUP BY first,
         last""",
        )

    def test_order_bys_not_matching_ingredients(self):
        """If an order_by is not found in dimensions+metrics, we ignore it"""
        recipe = self.recipe().metrics("age").dimensions("first").order_by("last")
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
GROUP BY first""",
        )
        self.assertRecipeSQLNotContains(recipe, "last")
        self.assertEqual(recipe.all()[0].first, "hi")
        self.assertEqual(recipe.all()[0].age, 15)
        self.assertEqual(recipe.stats.rows, 1)

    def test_from_config_extra_kwargs(self):
        config = {"dimensions": ["last"], "metrics": ["age"]}
        recipe = Recipe.from_config(self.shelf, config, order_by=["last"]).session(
            self.session
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
GROUP BY last
ORDER BY last""",
        )

    def test_recipe_empty(self):
        recipe = self.recipe()
        with self.assertRaises(BadRecipe):
            recipe.all()

    def test_recipe_multi_tables(self):
        """All ingredients in the recipe must use the same table"""
        dim = Dimension(self.scores_table.c.username)
        recipe = self.recipe().dimensions("last", dim).metrics("age")
        with self.assertRaises(BadRecipe):
            recipe.all()

    def test_recipe_as_table(self):
        recipe = self.recipe().dimensions("last").metrics("age")
        tbl = recipe.as_table()
        self.assertEqual(tbl.name, recipe._id)

        tbl = recipe.as_table(name="foo")
        self.assertEqual(tbl.name, "foo")

    def test_filter(self):
        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("last")
            .filters(self.basic_table.c.age > 2)
            .order_by("last")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 2
GROUP BY last
ORDER BY last""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            last,age,last_id
            fred,10,fred
            there,5,there
            """,
        )

    def test_having(self):
        hv = Having(func.sum(self.basic_table.c.age) < 10)
        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("last")
            .filters(self.basic_table.c.age > 2)
            .filters(hv)
            .order_by("last")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.last AS last,
       sum(foo.age) AS age
FROM foo
WHERE foo.age > 2
GROUP BY last
HAVING sum(foo.age) < 10
ORDER BY last""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            last,age,last_id
            there,5,there
            """,
        )


class StatsTestCase(RecipeTestCase):
    def test_stats(self):
        recipe = self.recipe().metrics("age").dimensions("last")
        recipe.all()

        # Stats are ready after the recipe is run
        self.assertEqual(recipe.stats.rows, 2)
        self.assertLess(recipe.stats.dbtime, 1.0)
        self.assertLess(recipe.stats.enchanttime, 1.0)
        self.assertFalse(recipe.stats.from_cache)


class NestedRecipeTestCase(RecipeTestCase):
    def test_nested_recipe(self):
        recipe = self.recipe().metrics("age").dimensions("last")
        from recipe import Metric, Dimension

        subq = recipe.subquery(name="anon")
        nested_shelf = Shelf(
            {"age": Metric(func.sum(subq.c.age)), "last": Dimension(subq.c.last)}
        )
        r = self.recipe(shelf=nested_shelf).dimensions("last").metrics("age")
        self.assertRecipeSQL(
            r,
            """SELECT anon.last AS last,
       sum(anon.age) AS age
FROM
  (SELECT foo.last AS last,
          sum(foo.age) AS age
   FROM foo
   GROUP BY last) AS anon
GROUP BY last""",
        )
        self.assertEqual(len(r.all()), 2)


class SelectFromTestCase(RecipeTestCase):
    def test_recipe_with_select_from(self):
        j = join(
            self.census_table,
            self.state_fact_table,
            self.census_table.c.state == self.state_fact_table.c.name,
        )

        from recipe import Dimension, Metric

        shelf = Shelf(
            {
                "region": Dimension(self.state_fact_table.c.census_region_name),
                "pop": Metric(func.sum(self.census_table.c.pop2000)),
            }
        )

        r = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("region")
            .metrics("pop")
            .select_from(j)
        )

        self.assertRecipeSQL(
            r,
            """SELECT state_fact.census_region_name AS region,
       sum(census.pop2000) AS pop
FROM census
JOIN state_fact ON census.state = state_fact.name
GROUP BY region""",
        )
        self.assertRecipeCSV(
            r,
            """
            region,pop,region_id
            Northeast,609480,Northeast
            South,5685230,South
            """,
        )


class ShelfSelectFromTestCase(RecipeTestCase):
    def test_recipe_with_select_from(self):
        from recipe import Dimension, Metric

        shelf = Shelf(
            {
                "region": Dimension(self.state_fact_table.c.census_region_name),
                "pop": Metric(func.sum(self.census_table.c.pop2000)),
            },
            select_from=join(
                self.census_table,
                self.state_fact_table,
                self.census_table.c.state == self.state_fact_table.c.name,
            ),
        )

        self.assertTrue(shelf.Meta.select_from is not None)

        r = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("region")
            .metrics("pop")
        )
        self.assertTrue(r._select_from is not None)

        self.assertRecipeSQL(
            r,
            """SELECT state_fact.census_region_name AS region,
       sum(census.pop2000) AS pop
FROM census
JOIN state_fact ON census.state = state_fact.name
GROUP BY region""",
        )
        self.assertRecipeCSV(
            r,
            """
            region,pop,region_id
            Northeast,609480,Northeast
            South,5685230,South
            """,
        )

    def test_recipe_as_subquery(self):
        """Use a recipe subquery as a source for generating a new shelf."""

        recipe = (
            Recipe(shelf=self.census_shelf, session=self.session)
            .metrics("pop2000")
            .dimensions("state")
        )

        # Another approach is to use
        subq = recipe.subquery()
        recipe_subquery_shelf = Shelf({"pop": Metric(func.avg(subq.c.pop2000))})
        r2 = Recipe(shelf=recipe_subquery_shelf, session=self.session).metrics("pop")
        self.assertRecipeSQL(
            r2,
            """SELECT avg(anon_1.pop2000) AS pop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000
   FROM census
   GROUP BY state) AS anon_1""",
        )
        self.assertRecipeCSV(r2, "pop\n3147355.0")


class CacheContextTestCase(RecipeTestCase):
    def test_cache_context(self):
        recipe = self.recipe().metrics("age").dimensions("last")
        recipe.cache_context = "foo"

        self.assertEqual(len(recipe.all()), 2)
        self.assertEqual(recipe._cauldron["last"].cache_context, "foo")
