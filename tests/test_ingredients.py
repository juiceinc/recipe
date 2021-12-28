# -*- coding: utf-8 -*-

import pytest
from sqlalchemy import case, distinct, func, cast, Float
from .test_base import RecipeTestCase
from recipe import (
    BadIngredient,
    InvalidColumnError,
    Dimension,
    DivideMetric,
    Filter,
    Having,
    IdValueDimension,
    Ingredient,
    LookupDimension,
    Metric,
    WtdAvgMetric,
)
from recipe.schemas.config_constructors import (
    ingredient_from_unvalidated_dict as ingredient_from_dict,
)
from recipe.schemas.config_constructors import parse_unvalidated_field as parse_field
from recipe.schemas.config_constructors import SAFE_DIVISON_EPSILON
from recipe.utils import filter_to_string


class TestIngredients(RecipeTestCase):
    def setup(self):
        self.shelf = self.mytable_shelf

    def test_ingredient_init(self):
        ingr = Ingredient()
        self.assertEqual(len(ingr.id), 12)
        self.assertIsInstance(ingr.columns, list)

        # Ids can be str
        ingr = Ingredient(id="ვეპხის")
        self.assertEqual(ingr.id, "ვეპხის")

        # Extra properties are stored in a AttrDict
        ingr = Ingredient(foo=2)
        self.assertEqual(ingr.meta.foo, 2)
        self.assertEqual(ingr.meta["foo"], 2)

        with self.assertRaises(BadIngredient):
            # Formatters must be list
            ingr = Ingredient(formatters="moo")

        with self.assertRaises(BadIngredient):
            # Formatters must be list
            ingr = Ingredient(formatters=2)

        with self.assertRaises(BadIngredient):
            # There must be the same number of column suffixes as columns
            ingr = Ingredient(
                columns=[self.basic_table.c.first, self.basic_table.c.last]
            )
            ingr.make_column_suffixes()

        with self.assertRaises(BadIngredient):
            # There must be the same number of column suffixes as columns
            ingr = Ingredient(
                column_suffixes=("foo",),
                columns=[self.basic_table.c.first, self.basic_table.c.last],
            )
            ingr.make_column_suffixes()

    def test_repr(self):
        ingr = Ingredient(
            column_suffixes=("_foo", "_moo"),
            columns=[self.basic_table.c.first, self.basic_table.c.last],
        )
        s = ingr.__repr__()
        self.assertTrue(
            s.startswith("(Ingredient)") and s.endswith("foo.first foo.last")
        )

    def test_comparisons(self):
        """Items sort in a fixed order"""
        ingr = Ingredient(columns=[self.basic_table.c.first], id=1)
        ingr2 = Ingredient(columns=[self.basic_table.c.first], id=2)
        dim = Dimension(self.basic_table.c.first, id=3)
        met = Metric(func.sum(self.basic_table.c.first), id=4)
        met2 = Metric(func.sum(self.basic_table.c.first), id=2)
        filt = Filter(self.basic_table.c.first < "h", id=92)
        hav = Having(func.sum(self.basic_table.c.first) < 3, id=2)

        items = [filt, hav, met2, met, ingr, dim, ingr2]
        self.assertNotEqual(ingr, ingr2)
        self.assertLess(dim, met)
        self.assertLess(met, filt)
        self.assertLess(filt, hav)
        self.assertLess(dim, hav)
        self.assertEqual(sorted(items), [dim, met2, met, filt, hav, ingr, ingr2])

    def test_ingredient_make_column_suffixes(self):
        # make_column_suffixes
        # There must be the same number of column suffixes as columns
        ingr = Ingredient(
            column_suffixes=("_foo", "_moo"),
            columns=[self.basic_table.c.first, self.basic_table.c.last],
        )
        self.assertEqual(ingr.make_column_suffixes(), ("_foo", "_moo"))

        ingr = Dimension(self.basic_table.c.first, formatters=[lambda x: x + "foo"])
        self.assertEqual(ingr.make_column_suffixes(), ("_raw",))

    def test_cache_context(self):
        # Cache context is saved
        ingr = Ingredient(cache_context="foo")
        self.assertEqual(ingr.cache_context, "foo")

    def test_ingredient_describe(self):
        # .describe()
        ingr = Ingredient(
            id="foo", columns=[self.basic_table.c.first, self.basic_table.c.last]
        )
        self.assertEqual(ingr.describe(), "(Ingredient)foo foo.first foo.last")

        ingr = Dimension(self.basic_table.c.first, id="foo")
        self.assertEqual(ingr.describe(), "(Dimension)foo foo.first")

    def test_ingredient_cauldron_extras(self):
        ingr = Ingredient(
            id="foo", columns=[self.basic_table.c.first, self.basic_table.c.last]
        )
        extras = list(ingr.cauldron_extras)
        self.assertEqual(len(extras), 0)

        ingr = Metric(
            self.basic_table.c.first, id="foo", formatters=[lambda x: x + "foo"]
        )
        extras = list(ingr.cauldron_extras)
        self.assertEqual(extras[0][0], "foo")
        self.assertEqual(len(extras), 1)

    def test_ingredient_cmp(self):
        """Ingredients are sorted by id"""
        ingra = Ingredient(id="b", columns=[self.basic_table.c.first])
        ingrb = Ingredient(id="a", columns=[self.basic_table.c.last])
        assert ingrb < ingra


class TestIngredientBuildFilter(RecipeTestCase):
    def eval_valid_filters(self, data):
        for dim, value, operator, expected_sql in data:
            filt = dim.build_filter(value, operator=operator)
            self.assertEqual(filter_to_string(filt), expected_sql)

    def eval_invalid_filters(self, data):
        for dim, value, operator in data:
            with self.assertRaises(ValueError):
                dim.build_filter(value, operator=operator)

    def test_scalar_filter(self):
        """Test scalar filters on a string dimension"""
        strdim = Dimension(self.basic_table.c.first)
        numdim = Dimension(self.basic_table.c.age)
        datedim = Dimension(self.basic_table.c.birth_date)
        dtdim = Dimension(self.basic_table.c.dt)

        self.assertEqual(strdim.datatype, "str")
        self.assertEqual(numdim.datatype, "num")
        self.assertEqual(datedim.datatype, "date")
        self.assertEqual(dtdim.datatype, "datetime")

        # Test building scalar filters
        data = [
            (strdim, "moo", None, "foo.first = 'moo'"),
            (strdim, "moo", "eq", "foo.first = 'moo'"),
            (strdim, "moo", "ne", "foo.first != 'moo'"),
            (strdim, "moo", "lt", "foo.first < 'moo'"),
            (strdim, "moo", "lte", "foo.first <= 'moo'"),
            (strdim, "moo", "gt", "foo.first > 'moo'"),
            (strdim, "moo", "gte", "foo.first >= 'moo'"),
            (strdim, "moo", "is", "foo.first IS 'moo'"),
            (strdim, "moo", "isnot", "foo.first IS NOT 'moo'"),
            (strdim, "moo", "like", "foo.first LIKE 'moo'"),
            (strdim, "moo", "ilike", "lower(foo.first) LIKE lower('moo')"),
            # Numbers get converted to strings
            (strdim, 5, "ilike", "lower(foo.first) LIKE lower('5')"),
            # Nones get converted to IS
            (strdim, None, None, "foo.first IS NULL"),
            (strdim, None, "eq", "foo.first IS NULL"),
            (strdim, "Τη γλώσ", "eq", "foo.first = 'Τη γλώσ'"),
            # Numeric dimension
            (numdim, "moo", None, "CAST(foo.age AS VARCHAR) = 'moo'"),
            (numdim, "moo", "eq", "CAST(foo.age AS VARCHAR) = 'moo'"),
            (numdim, "moo", "ne", "CAST(foo.age AS VARCHAR) != 'moo'"),
            (numdim, "moo", "lt", "CAST(foo.age AS VARCHAR) < 'moo'"),
            (numdim, "moo", "lte", "CAST(foo.age AS VARCHAR) <= 'moo'"),
            (numdim, "moo", "gt", "CAST(foo.age AS VARCHAR) > 'moo'"),
            (numdim, "moo", "gte", "CAST(foo.age AS VARCHAR) >= 'moo'"),
            (numdim, "moo", "is", "CAST(foo.age AS VARCHAR) IS 'moo'"),
            (numdim, "moo", "isnot", "CAST(foo.age AS VARCHAR) IS NOT 'moo'"),
            (numdim, "moo", "like", "CAST(foo.age AS VARCHAR) LIKE 'moo'"),
            (
                numdim,
                "moo",
                "ilike",
                "lower(CAST(foo.age AS VARCHAR)) LIKE lower('moo')",
            ),
            # Nones get converted to IS
            (numdim, None, None, "foo.age IS NULL"),
            (numdim, None, "eq", "foo.age IS NULL"),
            (numdim, "Τη γλώσ", "eq", "CAST(foo.age AS VARCHAR) = 'Τη γλώσ'"),
            # Numeric dimension with number value
            (numdim, 5, None, "foo.age = 5"),
            (numdim, 5, "eq", "foo.age = 5"),
            (numdim, 5, "ne", "foo.age != 5"),
            (numdim, 5, "lt", "foo.age < 5"),
            (numdim, 5, "lte", "foo.age <= 5"),
            (numdim, 5, "gt", "foo.age > 5"),
            (numdim, 5, "gte", "foo.age >= 5"),
            (numdim, 5, "is", "foo.age IS 5"),
            (numdim, 5, "isnot", "foo.age IS NOT 5"),
            (numdim, 5, "like", "foo.age LIKE '5'"),
            (numdim, 5, "ilike", "lower(foo.age) LIKE lower('5')"),
            # numdim,  Nones get converted to IS
            (numdim, None, None, "foo.age IS NULL"),
            (numdim, None, "eq", "foo.age IS NULL"),
            (numdim, "Τη γλώσ", None, "CAST(foo.age AS VARCHAR) = 'Τη γλώσ'"),
            # Dates
            (datedim, "2020-01-01", None, "foo.birth_date = '2020-01-01'"),
            (datedim, "2020-01-01", "eq", "foo.birth_date = '2020-01-01'"),
            (datedim, "2020-01-01T03:05", None, "foo.birth_date = '2020-01-01'"),
            (datedim, "2020-01-01T03:05", "eq", "foo.birth_date = '2020-01-01'"),
            # An unparsable date will be treated as a string
            (
                datedim,
                "2020-01-01T03:05X523",
                None,
                "CAST(foo.birth_date AS VARCHAR) = '2020-01-01T03:05X523'",
            ),
            (
                datedim,
                "2020-01-01T03:05X523",
                "eq",
                "CAST(foo.birth_date AS VARCHAR) = '2020-01-01T03:05X523'",
            ),
            # Evaluated as timestamp=0
            (datedim, 0, None, "foo.birth_date = '1970-01-01'"),
            # Datetimes
            (dtdim, "2020-01-01", None, "foo.dt = '2020-01-01 00:00:00'"),
            (dtdim, "2020-01-01T03:05", None, "foo.dt = '2020-01-01 03:05:00'"),
            (
                dtdim,
                "2020-01-01T03:05 UTC",
                None,
                "foo.dt = '2020-01-01 03:05:00+00:00'",
            ),
            (dtdim, "2020-01-01T03:05Z", None, "foo.dt = '2020-01-01 03:05:00+00:00'"),
            (
                dtdim,
                "2020-01-01T03:05 EST",
                None,
                "foo.dt = '2020-01-01 03:05:00-05:00'",
            ),
            (
                dtdim,
                "2020-01-01T03:05:01.123456 EST",
                None,
                "foo.dt = '2020-01-01 03:05:01.123456-05:00'",
            ),
            # Unparsable date will be treated as a string
            (
                dtdim,
                "2020-01-01T03:05X523",
                "eq",
                "CAST(foo.dt AS VARCHAR) = '2020-01-01T03:05X523'",
            ),
            (dtdim, 0, None, "foo.dt = '1970-01-01 00:00:00'"),
        ]

        baddata = [
            # Scalar operators must have scalar values
            (strdim, ["moo"], "eq"),
            (strdim, ["moo"], "lt"),
            # Unknown operator
            (strdim, "moo", "cows"),
            # Numeric dimension
            (numdim, ["moo"], "eq"),
            (numdim, ["moo"], "lt"),
            # Unknown operator
            (numdim, "moo", "cows"),
            # Scalar operators must have scalar values
            (numdim, [5], "eq"),
            (numdim, [5], "lt"),
            # Unknown operator
            (numdim, 5, "cows"),
        ]

        self.eval_valid_filters(data)
        self.eval_invalid_filters(baddata)

    def test_vector_filter(self):
        strdim = Dimension(self.basic_table.c.first)
        numdim = Dimension(self.basic_table.c.age)
        datedim = Dimension(self.basic_table.c.birth_date)

        self.assertEqual(strdim.datatype, "str")
        self.assertEqual(numdim.datatype, "num")
        self.assertEqual(datedim.datatype, "date")

        seconds_in_day = 24 * 60 * 60
        data = [
            (strdim, ["moo"], None, "foo.first IN ('moo')"),
            (strdim, ["moo", None], None, "foo.first IS NULL OR foo.first IN ('moo')"),
            (
                strdim,
                [None, "moo", None, None],
                None,
                "foo.first IS NULL OR foo.first IN ('moo')",
            ),
            (strdim, [None, None], None, "foo.first IS NULL"),
            # Values are sorted because recipe produces deterministic SQL
            (strdim, ["moo", "foo"], None, "foo.first IN ('foo', 'moo')"),
            (strdim, ["moo", "foo"], "in", "foo.first IN ('foo', 'moo')"),
            # Not in
            (strdim, ["moo", "foo"], "notin", "foo.first NOT IN ('foo', 'moo')"),
            (
                strdim,
                ["moo", None],
                "notin",
                "foo.first IS NOT NULL AND foo.first NOT IN ('moo')",
            ),
            (strdim, [None, None], "notin", "foo.first IS NOT NULL"),
            # Between values are not sorted
            (strdim, ["moo", "foo"], "between", "foo.first BETWEEN 'moo' AND 'foo'"),
            (
                datedim,
                ["2020-01-01", None, "2020-10-25"],
                None,
                "foo.birth_date IS NULL OR foo.birth_date IN ('2020-01-01', '2020-10-25')",
            ),
            (datedim, [0], None, "foo.birth_date IN ('1970-01-01')"),
            (
                datedim,
                [seconds_in_day + 0.123565, None, 0],
                None,
                "foo.birth_date IS NULL OR foo.birth_date IN ('1970-01-01', '1970-01-02')",
            ),
            (
                datedim,
                [seconds_in_day * 3],
                "notin",
                "foo.birth_date NOT IN ('1970-01-04')",
            ),
        ]

        baddata = [
            # Vector operators must have list values that match required length
            (strdim, "moo", "in"),
            (strdim, ["moo", "foo", "tru"], "between"),
            (strdim, ["moo"], "between"),
            # Unknown operator
            (strdim, ["moo"], "cows"),
        ]

        self.eval_valid_filters(data)
        self.eval_invalid_filters(baddata)

    def test_quickselects(self):
        d = Dimension(
            self.basic_table.c.first,
            quickselects=[
                {"name": "a", "condition": self.basic_table.c.first == "a"},
                {"name": "b", "condition": self.basic_table.c.last == "b"},
            ],
        )

        # Test building scalar filters
        filt = d.build_filter("a", operator="quickselect")
        self.assertEqual(filter_to_string(filt), "foo.first = 'a'")
        filt = d.build_filter("b", operator="quickselect")
        self.assertEqual(filter_to_string(filt), "foo.last = 'b'")

        with self.assertRaises(ValueError):
            filt = d.build_filter("c", operator="quickselect")

        d = Dimension(
            self.basic_table.c.first,
            quickselects=[
                {"name": "a", "condition": self.basic_table.c.first == "a"},
                {"name": "b", "condition": self.basic_table.c.last == "b"},
            ],
        )

        # Test building vector filters
        filt = d.build_filter(["a"], operator="quickselect")
        self.assertEqual(filter_to_string(filt), "foo.first = 'a'")
        filt = d.build_filter(["b"], operator="quickselect")
        self.assertEqual(filter_to_string(filt), "foo.last = 'b'")
        filt = d.build_filter(["a", "b"], operator="quickselect")
        self.assertEqual(filter_to_string(filt), "foo.first = 'a' OR foo.last = 'b'")
        filt = d.build_filter(["b", "a"], operator="quickselect")
        self.assertEqual(filter_to_string(filt), "foo.last = 'b' OR foo.first = 'a'")

        with self.assertRaises(ValueError):
            filt = d.build_filter(["c"], operator="quickselect")
        with self.assertRaises(ValueError):
            filt = d.build_filter([[]], operator="quickselect")
        with self.assertRaises(ValueError):
            filt = d.build_filter([2], operator="quickselect")
        with self.assertRaises(ValueError):
            filt = d.build_filter(["a", "b", "c"], operator="quickselect")


class TestFilter(RecipeTestCase):
    def test_filter_cmp(self):
        """Filters are compared on their filter expression"""
        filters = set()
        f1 = Filter(self.basic_table.c.first == "moo", id="f1")
        f2 = Filter(self.basic_table.c.first == "foo", id="f2")

        filters.add(f1)
        filters.add(f2)
        self.assertEqual(len(filters), 2)
        self.assertEqual(str(f1), "(Filter)f1 foo.first = 'moo'")

    def test_expression(self):
        f = Filter(self.basic_table.c.first == "foo")
        self.assertIsNotNone(f.expression)
        f.columns = []
        self.assertIsNotNone(f.expression)
        f.filters = []
        self.assertIsNone(f.expression)

    def test_filter_describe(self):
        f1 = Filter(self.basic_table.c.first == "moo", id="moo")
        self.assertEqual(f1.describe(), "(Filter)moo foo.first = 'moo'")


class TestHaving(RecipeTestCase):
    def test_having_cmp(self):
        """Filters are compared on their filter expression"""
        havings = set()
        f1 = Having(func.sum(self.basic_table.c.age) > 2, id="h1")
        f2 = Having(func.sum(self.basic_table.c.age) > 3, id="h2")

        havings.add(f1)
        havings.add(f2)
        self.assertEqual(len(havings), 2)

        self.assertEqual(str(f1), "(Having)h1 sum(foo.age) > :sum_1")

    def test_expression(self):
        h = Having(func.sum(self.basic_table.c.age) > 2)
        self.assertIsNotNone(h.expression)
        h.columns = []
        self.assertIsNotNone(h.expression)
        h.filters = []
        self.assertIsNotNone(h.expression)
        h.havings = []
        self.assertIsNone(h.expression)

    def test_having_describe(self):
        f1 = Having(func.sum(self.basic_table.c.age) > 2, id="moo")
        self.assertEqual(f1.describe(), "(Having)moo sum(foo.age) > :sum_1")


class TestDimension(RecipeTestCase):
    def test_init(self):
        d = Dimension(self.basic_table.c.first)
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 1)

        # Dimension with different id and value expressions
        d = Dimension(self.basic_table.c.first, id_expression=self.basic_table.c.last)
        self.assertEqual(len(d.columns), 2)
        self.assertEqual(len(d.group_by), 2)

    def test_dimension_order_by(self):
        d = Dimension(self.basic_table.c.first)
        self.assertEqual(len(list(d.order_by_columns)), 1)

        # Dimension with different id and value expressions
        d = Dimension(self.basic_table.c.first, id_expression=self.basic_table.c.last)
        self.assertEqual(len(list(d.order_by_columns)), 2)
        # Order by value expression then id expression
        self.assertEqual(list(map(str, d.order_by_columns)), [d.id, d.id + "_id"])

        # Extra roles DO participate in ordering
        d = Dimension(
            self.basic_table.c.first,
            id_expression=self.basic_table.c.last,
            age_expression=self.basic_table.c.age,
            id="moo",
        )
        self.assertEqual(len(list(d.order_by_columns)), 3)
        self.assertEqual(
            list(map(str, d.order_by_columns)), ["moo_age", "moo", "moo_id"]
        )

        # Extra roles DO participate in ordering, order_by_expression is always first
        d = Dimension(
            self.basic_table.c.first,
            id_expression=self.basic_table.c.last,
            age_expression=self.basic_table.c.age,
            order_by_expression=self.basic_table.c.age,
            id="moo",
        )
        self.assertEqual(len(list(d.order_by_columns)), 4)
        self.assertEqual(
            list(map(str, d.order_by_columns)),
            ["moo_order_by", "moo_age", "moo", "moo_id"],
        )

        d = Dimension(
            self.basic_table.c.first,
            id_expression=self.basic_table.c.last,
            zed_expression=self.basic_table.c.age,
            order_by_expression=self.basic_table.c.age,
            id="moo",
        )
        self.assertEqual(len(list(d.order_by_columns)), 4)
        self.assertEqual(
            list(map(str, d.order_by_columns)),
            ["moo_order_by", "moo_zed", "moo", "moo_id"],
        )

        # Default ordering can be set to descending
        d = Dimension(
            self.basic_table.c.first,
            id_expression=self.basic_table.c.last,
            zed_expression=self.basic_table.c.age,
            order_by_expression=self.basic_table.c.age,
            ordering="desc",
            id="moo",
        )
        self.assertEqual(len(list(d.order_by_columns)), 4)
        self.assertEqual(
            list(map(str, d.order_by_columns)),
            ["moo_order_by DESC", "moo_zed DESC", "moo DESC", "moo_id DESC"],
        )

    def test_dimension_cauldron_extras(self):
        d = Dimension(self.basic_table.c.first, id="moo")
        extras = list(d.cauldron_extras)
        self.assertEqual(len(extras), 1)
        # id gets injected in the response
        self.assertEqual(extras[0][0], "moo_id")

        d = Dimension(
            self.basic_table.c.first, id="moo", formatters=[lambda x: x + "moo"]
        )
        extras = list(d.cauldron_extras)
        self.assertEqual(len(extras), 2)
        # formatted value and id gets injected in the response
        self.assertEqual(extras[0][0], "moo")
        self.assertEqual(extras[1][0], "moo_id")

    def test_dimension_extra_roles(self):
        """Creating a dimension with extra roles"""
        d = Dimension(
            self.basic_table.c.first,
            id_expression=self.basic_table.c.last,
            age_expression=self.basic_table.c.age,
            id="moo",
        )
        extras = list(d.cauldron_extras)
        self.assertEqual(len(extras), 1)
        # id gets injected in the response
        self.assertEqual(extras[0][0], "moo_id")
        self.assertEqual(d.role_keys, ["id", "value", "age"])
        self.assertEqual(len(d.group_by), 3)
        self.assertEqual(len(d.columns), 3)
        self.assertEqual(d.make_column_suffixes(), ("_id", "", "_age"))

    def test_dimension_with_lookup(self):
        """Creating a dimension with extra roles"""
        # Dimension lookup should be a dict
        with self.assertRaises(BadIngredient):
            d = Dimension(self.basic_table.c.first, lookup="mouse", id="moo")

        d = Dimension(self.basic_table.c.first, lookup={"man": "mouse"}, id="moo")
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 1)
        self.assertEqual(len(d.formatters), 1)

        # Existing formatters are preserved
        d = Dimension(
            self.basic_table.c.first,
            lookup={"man": "mouse"},
            id="moo",
            formatters=[lambda x: x + "moo"],
        )
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 1)
        self.assertEqual(len(d.formatters), 2)


class TestIdValueDimension(RecipeTestCase):
    def test_init(self):
        # IdValueDimension should have two params
        with self.assertRaises(TypeError):
            d = IdValueDimension(self.basic_table.c.first)

        d = IdValueDimension(self.basic_table.c.first, self.basic_table.c.last)
        self.assertEqual(len(d.columns), 2)
        self.assertEqual(len(d.group_by), 2)

    def test_dimension_cauldron_extras(self):
        d = IdValueDimension(
            self.basic_table.c.first, self.basic_table.c.last, id="moo"
        )
        extras = list(d.cauldron_extras)
        self.assertEqual(len(extras), 1)
        # id gets injected in the response
        self.assertEqual(extras[0][0], "moo_id")

        d = IdValueDimension(
            self.basic_table.c.first,
            self.basic_table.c.last,
            id="moo",
            formatters=[lambda x: x + "moo"],
        )
        extras = list(d.cauldron_extras)
        self.assertEqual(len(extras), 2)
        # formatted value and id gets injected in the response
        self.assertEqual(extras[0][0], "moo")
        self.assertEqual(extras[1][0], "moo_id")

    def test_dimension_roles_cauldron_extras(self):
        """Creating a dimension with roles performs the same as
        IdValueDimension"""
        d = Dimension(
            self.basic_table.c.first, id_expression=self.basic_table.c.last, id="moo"
        )
        extras = list(d.cauldron_extras)
        self.assertEqual(len(extras), 1)
        # id gets injected in the response
        self.assertEqual(extras[0][0], "moo_id")

        d = Dimension(
            self.basic_table.c.first,
            id_expression=self.basic_table.c.last,
            id="moo",
            formatters=[lambda x: x + "moo"],
        )
        extras = list(d.cauldron_extras)
        self.assertEqual(len(extras), 2)
        # formatted value and id gets injected in the response
        self.assertEqual(extras[0][0], "moo")
        self.assertEqual(extras[1][0], "moo_id")


class TestLookupDimension(RecipeTestCase):
    """LookupDimension is deprecated and this feature is available in
    Dimension. See TestDimension.test_dimension_with_lookup for equivalent
    test on Dimension."""

    def test_init(self):
        # IdValueDimension should have two params
        with self.assertRaises(TypeError):
            d = LookupDimension(self.basic_table.c.first)

        # Dimension lookup should be a dict
        with self.assertRaises(BadIngredient):
            d = LookupDimension(self.basic_table.c.first, lookup="mouse")

        # Lookup dimension injects a formatter in the first position
        d = LookupDimension(self.basic_table.c.first, lookup={"hi": "there"})
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 1)
        self.assertEqual(len(d.formatters), 1)

        # Existing formatters are preserved
        d = LookupDimension(
            self.basic_table.c.first,
            lookup={"hi": "there"},
            formatters=[lambda x: x + "moo"],
        )
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 1)
        self.assertEqual(len(d.formatters), 2)

        # The lookup formatter is injected before any existing formatters
        def fmt(value):
            return value + "moo"

        d = LookupDimension(
            self.basic_table.c.first, lookup={"hi": "there"}, formatters=[fmt]
        )
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 1)
        self.assertEqual(len(d.formatters), 2)
        assert d.formatters[-1] is fmt


class TestMetric(RecipeTestCase):
    def test_init(self):
        # Metric should have an expression
        with self.assertRaises(TypeError):
            d = Metric()

        d = Metric(func.sum(self.basic_table.c.age))
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 0)
        self.assertEqual(len(d.filters), 0)

    def test_expression(self):
        d = Metric(func.sum(self.basic_table.c.age))
        assert d.expression is not None

        d.columns = []
        assert d.expression is None


class TestDivideMetric(RecipeTestCase):
    def test_init(self):
        # DivideMetric should have a two expressions
        with self.assertRaises(TypeError):
            d = DivideMetric()

        with self.assertRaises(TypeError):
            d = DivideMetric(func.sum(self.basic_table.c.age))

        d = DivideMetric(
            func.sum(self.basic_table.c.age), func.sum(self.basic_table.c.age)
        )
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 0)
        self.assertEqual(len(d.filters), 0)

        # Generate numerator / (denominator+epsilon) by default
        assert (
            str(d.columns[0]) == "CAST(sum(foo.age) AS FLOAT) / ("
            "coalesce("
            "CAST(sum(foo.age) AS FLOAT), "
            ":coalesce_1) + :coalesce_2)"
        )

        # Generate if denominator == 0 then 'zero' else numerator / denominator
        d = DivideMetric(
            func.sum(self.basic_table.c.age),
            func.sum(self.basic_table.c.age),
            ifzero="zero",
        )
        assert (
            str(d.columns[0])
            == "CASE WHEN (CAST(sum(foo.age) AS FLOAT) = :param_1) THEN "
            ":param_2 ELSE CAST(sum(foo.age) AS FLOAT) / "
            "CAST(sum(foo.age) AS FLOAT) END"
        )


class TestWtdAvgMetric(RecipeTestCase):
    def test_init(self):
        # WtdAvgMetric should have a two expressions
        with self.assertRaises(TypeError):
            d = WtdAvgMetric()

        with self.assertRaises(TypeError):
            d = WtdAvgMetric(self.basic_table.c.age)

        d = WtdAvgMetric(self.basic_table.c.age, self.basic_table.c.age)
        self.assertEqual(len(d.columns), 1)
        self.assertEqual(len(d.group_by), 0)
        self.assertEqual(len(d.filters), 0)

        # Generate numerator / (denominator+epsilon) by default
        assert (
            str(d.columns[0]) == "CAST(sum(foo.age * foo.age) AS FLOAT) / "
            "(coalesce(CAST(sum(foo.age) AS FLOAT), :coalesce_1) "
            "+ :coalesce_2)"
        )
