from copy import copy

import pytest
from sqlalchemy import join
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement
from yaml import safe_load

from recipe import (
    AutomaticShelf,
    InvalidColumnError,
    BadRecipe,
    Dimension,
    Metric,
    Filter,
    Shelf,
)
import recipe
from recipe.ingredients import Ingredient, InvalidIngredient
from recipe.shelf import introspect_table
from recipe.schemas.utils import find_column

from .test_base import RecipeTestCase


class FindColumnTestCase(RecipeTestCase):
    def test_find_column_from_recipe(self):
        """Can find columns in a recipe."""
        content = """
        state:
            kind: Dimension
            field: state
        sex:
            kind: Dimension
            field: sex
        age:
            kind: Dimension
            field: age
        sum_pop2000:
            kind: Metric
            field: pop2000
        pop2008:
            kind: Metric
            field: pop2008
        ttlpop:
            kind: Metric
            field: pop2000 + pop2008

        """
        shelf = Shelf.from_yaml(content, self.census_table)
        recipe = self.recipe(shelf=shelf).metrics("sum_pop2000").dimensions("state")
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state,
       sum(census.pop2000) AS sum_pop2000
FROM census
GROUP BY state"""
        )

        col = find_column(recipe, "state")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        with self.assertRaises(InvalidColumnError):
            find_column(recipe, "census_state")
        with self.assertRaises(InvalidColumnError):
            find_column(recipe, "foo")

        col = find_column(recipe, "sum_pop2000")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        with self.assertRaises(InvalidColumnError):
            find_column(recipe, "pop2000")

    def test_find_column_from_table_mytable(self):
        """SQLALchemy ORM Tables can be used and return
        InstrumentedAttributes"""
        col = find_column(self.basic_table, "first")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col2 = find_column(self.basic_table, "foo_first")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))
        self.assertEqual(col, col2)

        col = find_column(self.basic_table, "last")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col = find_column(self.basic_table, "age")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col = find_column(self.basic_table, "foo_age")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        with self.assertRaises(InvalidColumnError):
            find_column(self.basic_table, "foo")

    def test_find_column_from_join(self):
        """Columns can be found in a join"""
        j = join(
            self.census_table,
            self.state_fact_table,
            self.census_table.c.state == self.state_fact_table.c.name,
        )

        # Names can be either the column name or the {tablename}_{column}
        col = find_column(j, "state")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col2 = find_column(j, "census_state")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))
        self.assertEqual(col, col2)

        # Names can be either the column name or the {tablename}_{column}
        col = find_column(j, "sex")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col2 = find_column(j, "census_sex")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))
        self.assertEqual(col, col2)

        col = find_column(j, "assoc_press")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        # Columns can be referenced as {tablename}_{column}
        col2 = find_column(j, "state_fact_assoc_press")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))
        self.assertEqual(col, col2)

        with self.assertRaises(InvalidColumnError):
            find_column(j, "foo")

    def test_find_column_from_invalid_type(self):
        """Columns can be found in a join"""
        with self.assertRaises(InvalidColumnError):
            find_column(1, "foo")
        with self.assertRaises(InvalidColumnError):
            find_column(self.basic_table.c.first, "foo")


class ShelfConstructionTestCase(RecipeTestCase):
    def test_pass_some_metadata(self):
        shelf = Shelf(metadata={"a": "hello"})
        self.assertEqual(shelf.Meta.metadata["a"], "hello")

    def test_Meta_is_not_shared(self):
        shelf = Shelf(metadata={"a": "hello"})
        shelf2 = Shelf(metadata={"b": "there"})
        self.assertEqual(shelf.Meta.metadata, {"a": "hello"})
        self.assertEqual(shelf2.Meta.metadata, {"b": "there"})


class ShelfTestCase(RecipeTestCase):
    def setUp(self):
        super().setUp()
        # Be sure we don't modify the shelf while testing
        self.shelf = copy(self.shelf)

    def test_find(self):
        """Find ingredients on the shelf"""
        ingredient = self.shelf.find("first", Dimension)
        self.assertEqual(ingredient.id, "first")

        # Raise if the wrong type
        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("first", Metric)

        # Raise if key not present in shelf
        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        # Raise if key is not an ingredient or string
        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        self.shelf["foo"] = Dimension(self.basic_table.c.last)
        ingredient = self.shelf.find("last", Dimension)
        self.assertEqual(ingredient.id, "last")

    def test_find_filter(self):
        self.shelf["age_gt_20"] = Filter(self.basic_table.c.age > 20)

        ingredient = self.shelf.find("age_gt_20", Filter)
        self.assertEqual(ingredient.id, "age_gt_20")

        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("age_gt_20", Dimension)

    def test_repr(self):
        """Find ingredients on the shelf"""
        print(self.shelf.__repr__())
        assert (
            self.shelf.__repr__()
            == """(Dimension)first foo.first
(Dimension)firstlast foo.first foo.last
(Dimension)last foo.last
(Metric)age sum(foo.age)"""
        )

    def test_keys(self):
        self.assertEqual(
            sorted(self.shelf.keys()), ["age", "first", "firstlast", "last"]
        )

    def test_update(self):
        """Shelves can be updated with other shelves"""
        new_shelf = Shelf({"squee": Dimension(self.basic_table.c.first)})
        self.assertEqual(len(self.shelf), 4)
        self.shelf.update(new_shelf)
        self.assertEqual(len(self.shelf), 5)

    def test_update_key_value(self):
        """Shelves can be built with key_values and updated"""
        new_shelf = Shelf(squee=Dimension(self.basic_table.c.first))
        self.assertEqual(len(self.shelf), 4)
        self.shelf.update(new_shelf)
        self.assertEqual(len(self.shelf), 5)
        assert isinstance(self.shelf.get("squee"), Dimension)

    def test_update_key_value_direct(self):
        """Shelves can be updated directly with key_value"""
        self.assertEqual(len(self.shelf), 4)
        self.shelf.update(squee=Dimension(self.basic_table.c.first))
        self.assertEqual(len(self.shelf), 5)
        assert isinstance(self.shelf.get("squee"), Dimension)

    def test_brew(self):
        recipe_parts = self.shelf.brew_query_parts()
        self.assertEqual(len(recipe_parts["columns"]), 5)
        self.assertEqual(len(recipe_parts["group_bys"]), 4)
        self.assertEqual(len(recipe_parts["filters"]), 0)
        self.assertEqual(len(recipe_parts["havings"]), 0)

    def test_anonymize(self):
        """We can save and store anonymization context"""
        assert self.shelf.Meta.anonymize is False
        self.shelf.Meta.anonymize = True
        assert self.shelf.Meta.anonymize is True

    def test_get(self):
        """Find ingredients on the shelf"""
        ingredient = self.shelf["first"]
        self.assertEqual(ingredient.id, "first")

        ingredient = self.shelf.get("first", None)
        self.assertEqual(ingredient.id, "first")

        ingredient = self.shelf.get("primo", None)
        assert ingredient is None

    def test_get_doesnt_mutate(self):
        """
        Sharing ingredients between shelves won't cause race conditions on
        their `.id` and `.anonymize` attributes.
        """
        ingr = Ingredient(id="b")
        shelf = Shelf({"a": ingr})
        self.assertEqual(shelf["a"].id, "a")
        self.assertEqual(ingr.id, "b")

    def test_add_to_shelf(self):
        """We can add an ingredient to a shelf"""
        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        self.shelf["foo"] = Dimension(self.basic_table.c.last)
        ingredient = self.shelf.find("last", Dimension)
        self.assertEqual(ingredient.id, "last")

    def test_setitem_type_error(self):
        """Only ingredients can be added to shelves."""
        with self.assertRaises(TypeError):
            self.shelf["foo"] = 3

    def test_use_type_error(self):
        """`use` requires Ingredients"""
        with self.assertRaises(TypeError):
            self.shelf.use(3)

    def test_clear(self):
        self.shelf = copy(self.shelf)
        self.assertEqual(len(self.shelf), 4)
        self.shelf.clear()
        self.assertEqual(len(self.shelf), 0)

    def test_dimension_ids(self):
        self.assertEqual(len(self.shelf), 4)
        self.assertEqual(len(self.shelf.dimension_ids), 3)
        self.assertEqual(
            sorted(self.shelf.dimension_ids), ["first", "firstlast", "last"]
        )

    def test_metric_ids(self):
        self.assertEqual(len(self.shelf.metric_ids), 1)
        self.assertEqual(self.shelf.metric_ids, ("age",))

    def test_filter_ids(self):
        self.assertEqual(len(self.shelf.filter_ids), 0)


class ShelfFromYamlTestCase(RecipeTestCase):
    def make_shelf(self, content, table=None):
        if table is None:
            table = self.basic_table
        self.shelf = Shelf.from_yaml(content, table)
        self.shelf.Meta.anonymize = False

    def setUp(self):
        super().setUp()
        self.make_shelf(
            """
first:
    kind: Dimension
    field: first
last:
    kind: Dimension
    field: last
age:
    kind: Metric
    field: age
"""
        )

    def test_find(self):
        """Find ingredients on the shelf"""
        ingredient = self.shelf.find("first", Dimension)
        self.assertEqual(ingredient.id, "first")

        # Raise if the wrong type
        with self.assertRaises(Exception):
            ingredient = self.shelf.find("first", Metric)

        # Raise if key not present in shelf
        with self.assertRaises(Exception):
            ingredient = self.shelf.find("foo", Dimension)

        # Raise if key is not an ingredient or string
        with self.assertRaises(Exception):
            ingredient = self.shelf.find(2.0, Dimension)

        with self.assertRaises(Exception):
            ingredient = self.shelf.find("foo", Dimension)

        with self.assertRaises(Exception):
            ingredient = self.shelf.find(2.0, Dimension)

        with self.assertRaises(Exception):
            ingredient = self.shelf.find("foo", Dimension)

        with self.assertRaises(Exception):
            ingredient = self.shelf.find("foo", Dimension)

        self.shelf["foo"] = Dimension(self.basic_table.c.last)
        ingredient = self.shelf.find("last", Dimension)
        self.assertEqual(ingredient.id, "last")

    def test_repr(self):
        """Find ingredients on the shelf"""
        assert (
            self.shelf.__repr__()
            == """(Dimension)first foo.first
(Dimension)last foo.last
(Metric)age sum(foo.age)"""
        )

    def test_update(self):
        """Shelves can be updated with other shelves"""
        new_shelf = Shelf({"squee": Dimension(self.basic_table.c.first)})
        self.assertEqual(len(self.shelf), 3)
        self.shelf.update(new_shelf)
        self.assertEqual(len(self.shelf), 4)

    def test_update_key_value(self):
        """Shelves can be built with key_values and updated"""
        new_shelf = Shelf(squee=Dimension(self.basic_table.c.first))
        self.assertEqual(len(self.shelf), 3)
        self.shelf.update(new_shelf)
        self.assertEqual(len(self.shelf), 4)
        assert isinstance(self.shelf.get("squee"), Dimension)

    def test_update_key_value_direct(self):
        """Shelves can be updated directly with key_value"""
        self.assertEqual(len(self.shelf), 3)
        self.shelf.update(squee=Dimension(self.basic_table.c.first))
        self.assertEqual(len(self.shelf), 4)
        assert isinstance(self.shelf.get("squee"), Dimension)

    def test_brew(self):
        recipe_parts = self.shelf.brew_query_parts()
        self.assertEqual(len(recipe_parts["columns"]), 3)
        self.assertEqual(len(recipe_parts["group_bys"]), 2)
        self.assertEqual(len(recipe_parts["filters"]), 0)
        self.assertEqual(len(recipe_parts["havings"]), 0)

    def test_anonymize(self):
        """We can save and store anonymization context"""
        assert self.shelf.Meta.anonymize is False
        self.shelf.Meta.anonymize = True
        assert self.shelf.Meta.anonymize is True

    def test_anonymize_keeps_ingredients_up_to_date(self):
        """Setting the anonymize attribute causes all ingredients to be
        updated.
        """
        assert self.shelf["first"].anonymize is False
        self.shelf.Meta.anonymize = True
        assert self.shelf["first"].anonymize is True

    def test_get(self):
        """Find ingredients on the shelf"""
        ingredient = self.shelf["first"]
        self.assertEqual(ingredient.id, "first")

        ingredient = self.shelf.get("first", None)
        self.assertEqual(ingredient.id, "first")

        ingredient = self.shelf.get("primo", None)
        assert ingredient is None

    def test_add_to_shelf(self):
        """We can add an ingredient to a shelf"""
        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        self.shelf["foo"] = Dimension(self.basic_table.c.last)
        ingredient = self.shelf.find("last", Dimension)
        self.assertEqual(ingredient.id, "last")

    def test_clear(self):
        self.assertEqual(len(self.shelf), 3)
        self.shelf.clear()
        self.assertEqual(len(self.shelf), 0)

    def test_invalid_kind(self):
        content = """
oldage:
    kind: Metric2
    field:
        value: age
"""
        with self.assertRaises(Exception):
            self.make_shelf(content)


class ShelfFromValidatedYamlTestCase(ShelfFromYamlTestCase):
    """Test that shelves are created correctly using
    sureberus validation.
    """

    def make_shelf(self, content, table=None):
        if table is None:
            table = self.basic_table
        self.shelf = Shelf.from_validated_yaml(content, table)
        self.shelf.Meta.anonymize = False

    def test_invalid_definition(self):
        content = """
oldage:
    kind: Metric
    field: age
invalid:
    kind: Dimension
    field: age
    # raw_field is a reserved name and can't be assigned in config.
    raw_field: first
"""
        self.make_shelf(content)
        assert isinstance(self.shelf["oldage"], Metric)
        assert isinstance(self.shelf["invalid"], InvalidIngredient)
        assert (
            self.shelf["invalid"].error["extra"]["details"]
            == "raw is a reserved role in dimensions"
        )


class ShelfFromConfigTestCase(ShelfFromValidatedYamlTestCase):
    def make_shelf(self, content, table=None):
        if table is None:
            table = self.basic_table
        obj = safe_load(content)
        self.shelf = Shelf.from_config(obj, table)


class TestShelfFromIntrospection(RecipeTestCase):
    """Test that shelves are created correctly using
    sureberus validation.
    """

    def test_shelf(self):
        """Cerberus validated shelf doesn't accept null."""
        content = """
state:
    kind: Dimension
    field: state
sex:
    kind: Dimension
    field: sex
age:
    kind: Dimension
    field: age
pop2000:
    kind: Metric
    field: pop2000
pop2008:
    kind: Metric
    field: pop2008
ttlpop:
    kind: Metric
    field: pop2000 + pop2008

"""
        shelf = Shelf.from_validated_yaml(content, self.census_table)

        recipe = self.recipe(shelf=shelf).metrics("pop2000").dimensions("state")
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY state"""
        )
        assert (
            recipe.dataset.tsv
            == """state	pop2000	state_id\r
Tennessee	5685230	Tennessee\r
Vermont	609480	Vermont\r
"""
        )


class AutomaticShelfTestCase(RecipeTestCase):
    def setUp(self):
        super().setUp()
        self.shelf = AutomaticShelf(self.basic_table)

    def test_auto_find(self):
        """Find ingredients on the shelf"""
        ingredient = self.shelf.find("first", Dimension)
        self.assertEqual(ingredient.id, "first")

        # Raise if the wrong type
        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("first", Metric)

        # Raise if key not present in shelf
        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        # Raise if key is not an ingredient or string
        with self.assertRaises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

    def test_get(self):
        """Find ingredients on the shelf"""
        ingredient = self.shelf["first"]
        self.assertEqual(ingredient.id, "first")

        ingredient = self.shelf.get("first", None)
        self.assertEqual(ingredient.id, "first")

        ingredient = self.shelf["age"]
        self.assertEqual(str(ingredient.columns[0]), "sum(foo.age)")

        ingredient = self.shelf.get("primo", None)
        assert ingredient is None

    def test_introspect_table(self):
        config = introspect_table(self.basic_table)
        self.assertEqual(
            config,
            {
                "age": {"field": "age", "kind": "Metric"},
                "first": {"field": "first", "kind": "Dimension"},
                "last": {"field": "last", "kind": "Dimension"},
            },
        )
