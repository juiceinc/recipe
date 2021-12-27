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
    def test_scalar_filter(self):
        """Test scalar filters on a string dimension"""
        d = Dimension(self.basic_table.c.first)

        # Test building scalar filters
        filt = d.build_filter("moo")
        self.assertEqual(filter_to_string(filt), "foo.first = 'moo'")
        filt = d.build_filter("moo", "eq")
        self.assertEqual(filter_to_string(filt), "foo.first = 'moo'")
        filt = d.build_filter("moo", "ne")
        self.assertEqual(filter_to_string(filt), "foo.first != 'moo'")
        filt = d.build_filter("moo", "lt")
        self.assertEqual(filter_to_string(filt), "foo.first < 'moo'")
        filt = d.build_filter("moo", "lte")
        self.assertEqual(filter_to_string(filt), "foo.first <= 'moo'")
        filt = d.build_filter("moo", "gt")
        self.assertEqual(filter_to_string(filt), "foo.first > 'moo'")
        filt = d.build_filter("moo", "gte")
        self.assertEqual(filter_to_string(filt), "foo.first >= 'moo'")
        filt = d.build_filter("moo", "is")
        self.assertEqual(filter_to_string(filt), "foo.first IS 'moo'")
        filt = d.build_filter("moo", "isnot")
        self.assertEqual(filter_to_string(filt), "foo.first IS NOT 'moo'")
        filt = d.build_filter("moo", "like")
        self.assertEqual(filter_to_string(filt), "foo.first LIKE 'moo'")
        filt = d.build_filter("moo", "ilike")
        self.assertEqual(filter_to_string(filt), "lower(foo.first) LIKE lower('moo')")
        # Numbers get stringified
        filt = d.build_filter(5, "ilike")
        self.assertEqual(filter_to_string(filt), "lower(foo.first) LIKE lower('5')")
        # None values get converted to IS
        filt = d.build_filter(None, "eq")
        self.assertEqual(filter_to_string(filt), "foo.first IS NULL")

        # str filter values are acceptable
        filt = d.build_filter(u"Τη γλώσ")
        self.assertEqual(filter_to_string(filt), "foo.first = 'Τη γλώσ'")

        # operator must agree with value
        with self.assertRaises(ValueError):
            filt = d.build_filter(["moo"], "eq")
        with self.assertRaises(ValueError):
            filt = d.build_filter(["moo"], "lt")

        # Unknown operator
        with self.assertRaises(ValueError):
            filt = d.build_filter(["moo"], "cows")

    def test_scalar_filter_on_int(self):
        """Test scalar filters on an integer dimension"""
        d = Dimension(self.basic_table.c.age)

        # Test building scalar filters
        filt = d.build_filter("moo")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) = 'moo'")
        filt = d.build_filter("moo", "eq")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) = 'moo'")
        filt = d.build_filter("moo", "ne")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) != 'moo'")
        filt = d.build_filter("moo", "lt")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) < 'moo'")
        filt = d.build_filter("moo", "lte")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) <= 'moo'")
        filt = d.build_filter("moo", "gt")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) > 'moo'")
        filt = d.build_filter("moo", "gte")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) >= 'moo'")
        filt = d.build_filter("moo", "is")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) IS 'moo'")
        filt = d.build_filter("moo", "isnot")
        self.assertEqual(
            filter_to_string(filt), "CAST(foo.age AS VARCHAR) IS NOT 'moo'"
        )
        filt = d.build_filter("moo", "like")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) LIKE 'moo'")
        filt = d.build_filter("moo", "ilike")
        assert (
            filter_to_string(filt)
            == "lower(CAST(foo.age AS VARCHAR)) LIKE lower('moo')"
        )
        # None values get converted to IS
        filt = d.build_filter(None, "eq")
        self.assertEqual(filter_to_string(filt), "foo.age IS NULL")

        # str filter values are acceptable
        filt = d.build_filter(u"Τη γλώσ")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) = 'Τη γλώσ'")

        # operator must agree with value
        with self.assertRaises(ValueError):
            d.build_filter(["moo"], "eq")
        with self.assertRaises(ValueError):
            d.build_filter(["moo"], "lt")

        # Unknown operator
        with self.assertRaises(ValueError):
            d.build_filter(["moo"], "cows")

    def test_scalar_filter_on_int_dim_int_value(self):
        """Test scalar filters on an integer dimension passing an integer value"""
        d = Dimension(self.basic_table.c.age)

        # Test building scalar filters
        filt = d.build_filter(5)
        self.assertEqual(filter_to_string(filt), "foo.age = 5")
        filt = d.build_filter(5, "eq")
        self.assertEqual(filter_to_string(filt), "foo.age = 5")
        filt = d.build_filter(5, "ne")
        self.assertEqual(filter_to_string(filt), "foo.age != 5")
        filt = d.build_filter(5, "lt")
        self.assertEqual(filter_to_string(filt), "foo.age < 5")
        filt = d.build_filter(5, "lte")
        self.assertEqual(filter_to_string(filt), "foo.age <= 5")
        filt = d.build_filter(5, "gt")
        self.assertEqual(filter_to_string(filt), "foo.age > 5")
        filt = d.build_filter(5, "gte")
        self.assertEqual(filter_to_string(filt), "foo.age >= 5")
        filt = d.build_filter(5, "is")
        self.assertEqual(filter_to_string(filt), "foo.age IS 5")
        filt = d.build_filter(5, "isnot")
        self.assertEqual(filter_to_string(filt), "foo.age IS NOT 5")
        # None values get converted to IS
        filt = d.build_filter(None, "eq")
        self.assertEqual(filter_to_string(filt), "foo.age IS NULL")

        # str filter values are acceptable
        filt = d.build_filter(u"Τη γλώσ")
        self.assertEqual(filter_to_string(filt), "CAST(foo.age AS VARCHAR) = 'Τη γλώσ'")

        # operator must agree with value
        with self.assertRaises(ValueError):
            d.build_filter(["moo"], "eq")
        with self.assertRaises(ValueError):
            d.build_filter(["moo"], "lt")

        # Unknown operator
        with self.assertRaises(ValueError):
            d.build_filter(["moo"], "cows")

    def test_vector_filter(self):
        d = Dimension(self.basic_table.c.first)

        # Test building scalar filters
        filt = d.build_filter(["moo"])
        self.assertEqual(filter_to_string(filt), "foo.first IN ('moo')")
        filt = d.build_filter(["moo", None])
        self.assertEqual(
            filter_to_string(filt), "foo.first IS NULL OR foo.first IN ('moo')"
        )
        filt = d.build_filter([None, "moo", None, None])
        self.assertEqual(
            filter_to_string(filt), "foo.first IS NULL OR foo.first IN ('moo')"
        )
        filt = d.build_filter([None, None])
        self.assertEqual(filter_to_string(filt), "foo.first IS NULL")

        filt = d.build_filter(["moo", "foo"])
        # Values are sorted
        self.assertEqual(filter_to_string(filt), "foo.first IN ('foo', 'moo')")
        filt = d.build_filter(["moo"], operator="in")
        self.assertEqual(filter_to_string(filt), "foo.first IN ('moo')")
        filt = d.build_filter(["moo"], operator="notin")
        self.assertEqual(filter_to_string(filt), "foo.first NOT IN ('moo')")
        filt = d.build_filter(["moo", None], operator="notin")
        assert (
            filter_to_string(filt)
            == "foo.first IS NOT NULL AND foo.first NOT IN ('moo')"
        )
        filt = d.build_filter([None, "moo", None], operator="notin")
        assert (
            filter_to_string(filt)
            == "foo.first IS NOT NULL AND foo.first NOT IN ('moo')"
        )
        filt = d.build_filter([None, None], operator="notin")
        self.assertEqual(filter_to_string(filt), "foo.first IS NOT NULL")
        filt = d.build_filter(["moo", "foo"], operator="between")
        self.assertEqual(filter_to_string(filt), "foo.first BETWEEN 'moo' AND 'foo'")

        with self.assertRaises(ValueError):
            d.build_filter("moo", "in")
        # Between must have 2 values
        with self.assertRaises(ValueError):
            d.build_filter(["moo", "foo", "tru"], operator="between")
        with self.assertRaises(ValueError):
            d.build_filter(["moo"], operator="between")

    def test_scalar_filter_date(self):
        d = Dimension(self.basic_table.c.birth_date)
        # Test building scalar filters
        filt = d.build_filter("2020-01-01")
        self.assertEqual(filter_to_string(filt), "foo.birth_date = '2020-01-01'")

        filt = d.build_filter("2020-01-01T03:05")
        self.assertEqual(filter_to_string(filt), "foo.birth_date = '2020-01-01'")

        # An unparsable date will be treated as a string
        filt = d.build_filter("2020-01-01T03:05X523")
        assert (
            filter_to_string(filt)
            == "CAST(foo.birth_date AS VARCHAR) = '2020-01-01T03:05X523'"
        )

        # Evaluated as timestamp=0
        filt = d.build_filter(0)
        self.assertEqual(filter_to_string(filt), "foo.birth_date = '1970-01-01'")

    def test_scalar_filter_datetime(self):
        d = Dimension(self.basic_table.c.dt)
        # Test building scalar filters
        filt = d.build_filter("2020-01-01")
        self.assertEqual(filter_to_string(filt), "foo.dt = '2020-01-01 00:00:00'")

        filt = d.build_filter("2020-01-01T03:05")
        self.assertEqual(filter_to_string(filt), "foo.dt = '2020-01-01 03:05:00'")

        filt = d.build_filter("2020-01-01T03:05 UTC")
        self.assertEqual(filter_to_string(filt), "foo.dt = '2020-01-01 03:05:00+00:00'")

        filt = d.build_filter("2020-01-01T04:06:01Z")
        self.assertEqual(filter_to_string(filt), "foo.dt = '2020-01-01 04:06:01+00:00'")

        filt = d.build_filter("2020-01-01T03:05 EST")
        self.assertEqual(filter_to_string(filt), "foo.dt = '2020-01-01 03:05:00-05:00'")

        filt = d.build_filter("2020-01-01T06:07:04.123456")
        self.assertEqual(
            filter_to_string(filt), "foo.dt = '2020-01-01 06:07:04.123456'"
        )

        # An unparsable date will be treated as a string
        filt = d.build_filter("2020-01-01T03:05X523")
        assert (
            filter_to_string(filt) == "CAST(foo.dt AS VARCHAR) = '2020-01-01T03:05X523'"
        )

        # Evaluated as timestamp=0
        filt = d.build_filter(0)
        self.assertEqual(filter_to_string(filt), "foo.dt = '1970-01-01 00:00:00'")

    def test_vector_filter_date(self):
        d = Dimension(self.basic_table.c.birth_date)
        # Test building scalar filters
        filt = d.build_filter(["2020-01-01", None, "2020-10-25"])
        assert (
            filter_to_string(filt)
            == "foo.birth_date IS NULL OR foo.birth_date IN ('2020-01-01', '2020-10-25')"
        )

        filt = d.build_filter([0])
        self.assertEqual(filter_to_string(filt), "foo.birth_date IN ('1970-01-01')")

        seconds_in_day = 24 * 60 * 60
        filt = d.build_filter([seconds_in_day + 0.123565, None, 0])
        assert (
            filter_to_string(filt)
            == "foo.birth_date IS NULL OR foo.birth_date IN ('1970-01-01', '1970-01-02')"
        )
        filt = d.build_filter([seconds_in_day * 3], operator="notin")
        self.assertEqual(filter_to_string(filt), "foo.birth_date NOT IN ('1970-01-04')")

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
        assert f.expression is not None

        f.columns = []
        assert f.expression is not None
        f.filters = []
        assert f.expression is None

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
        assert h.expression is not None

        h.columns = []
        assert h.expression is not None
        h.filters = []
        assert h.expression is not None
        h.havings = []
        assert h.expression is None

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


class TestIngredientFromObj(RecipeTestCase):
    def test_ingredient_from_obj(self):
        m = ingredient_from_dict({"kind": "Metric", "field": "age"}, self.basic_table)
        assert isinstance(m, Metric)

        d = ingredient_from_dict(
            {"kind": "Dimension", "field": "last"}, self.basic_table
        )
        assert isinstance(d, Dimension)

    def test_ingredient_from_dict(self):
        data = [
            ({"kind": "Metric", "field": "age"}, "(Metric)1 sum(foo.age)"),
            ({"kind": "Dimension", "field": "age"}, "(Dimension)1 foo.age"),
            (
                {"kind": "IdValueDimension", "field": "age", "id_field": "age"},
                "(Dimension)1 foo.age foo.age",
            ),
            (
                {
                    "kind": "Metric",
                    "field": {"value": "age", "condition": {"field": "age", "gt": 22}},
                },
                "(Metric)1 sum(CASE WHEN (foo.age > :age_1) THEN foo.age END)",
            ),
        ]

        for d, expected_result in data:
            m = ingredient_from_dict(d, self.basic_table)
            m.id = 1
            self.assertEqual(str(m), expected_result)

    def test_ingredient_from_bad_dict(self):
        bad_data = [
            # Missing required fields
            {"kind": "Metric"},
            # Bad kind
            {"kind": "MooCow", "field": "last"},
        ]
        for d in bad_data:
            with self.assertRaises(BadIngredient):
                ingredient_from_dict(d, self.basic_table)

    def test_ingredient_from_obj_with_meta(self):
        m = ingredient_from_dict(
            {"kind": "Metric", "field": "age", "format": "comma"}, self.basic_table
        )
        assert isinstance(m, Metric)
        self.assertEqual(m.meta.format, ",.0f")

    def test_ingredient_from_obj_with_missing_format_meta(self):
        m = ingredient_from_dict(
            {"kind": "Metric", "field": "age", "format": "foo"}, self.basic_table
        )
        assert isinstance(m, Metric)
        self.assertEqual(m.meta.format, "foo")


class TestParse(RecipeTestCase):
    def test_parse_field_aggregation(self):
        data = [
            # Basic fields
            ("age", func.sum(self.basic_table.c.age)),
            ({"value": "age"}, func.sum(self.basic_table.c.age)),
            # Aggregations
            ({"value": "age", "aggregation": "max"}, func.max(self.basic_table.c.age)),
            ({"value": "age", "aggregation": "sum"}, func.sum(self.basic_table.c.age)),
            ({"value": "age", "aggregation": "min"}, func.min(self.basic_table.c.age)),
            ({"value": "age", "aggregation": "avg"}, func.avg(self.basic_table.c.age)),
            (
                {"value": "age", "aggregation": "count_distinct"},
                func.count(distinct(self.basic_table.c.age)),
            ),
        ]
        for input_field, expected_result in data:
            result = parse_field(input_field, self.basic_table)
            self.assertEqual(str(result), str(expected_result))

    def test_parse_field_add_subtract(self):
        data = [
            # Basic fields
            (
                "first+last",
                func.sum(self.basic_table.c.first + self.basic_table.c.last),
            ),
            (
                "first-last",
                func.sum(self.basic_table.c.first - self.basic_table.c.last),
            ),
            (
                "first-last-first",
                func.sum(
                    self.basic_table.c.first
                    - self.basic_table.c.last
                    - self.basic_table.c.first
                ),
            ),
            (
                "first*last",
                func.sum(self.basic_table.c.first * self.basic_table.c.last),
            ),
            (
                "first/last",
                func.sum(
                    self.basic_table.c.first
                    / (
                        func.coalesce(cast(self.basic_table.c.last, Float), 0.0)
                        + SAFE_DIVISON_EPSILON
                    )
                ),
            ),
            (
                "first*last-first",
                func.sum(
                    self.basic_table.c.first * self.basic_table.c.last
                    - self.basic_table.c.first
                ),
            ),
            # Spacing doesn't matter
            (
                "first + last",
                func.sum(self.basic_table.c.first + self.basic_table.c.last),
            ),
            (
                "first -last",
                func.sum(self.basic_table.c.first - self.basic_table.c.last),
            ),
            (
                "first - last   -  first",
                func.sum(
                    self.basic_table.c.first
                    - self.basic_table.c.last
                    - self.basic_table.c.first
                ),
            ),
            (
                "first  *last",
                func.sum(self.basic_table.c.first * self.basic_table.c.last),
            ),
            (
                "first/  last",
                func.sum(
                    self.basic_table.c.first
                    / (
                        func.coalesce(cast(self.basic_table.c.last, Float), 0.0)
                        + SAFE_DIVISON_EPSILON
                    )
                ),
            ),
            (
                "first*  last /first",
                func.sum(
                    self.basic_table.c.first
                    * self.basic_table.c.last
                    / (
                        func.coalesce(cast(self.basic_table.c.first, Float), 0.0)
                        + SAFE_DIVISON_EPSILON
                    )
                ),
            ),
        ]
        for input_field, expected_result in data:
            result = parse_field(input_field, self.basic_table)
            self.assertEqual(str(result), str(expected_result))

    def test_parse_field_no_aggregations(self):
        data = [
            # Basic fields
            ("age", self.basic_table.c.age),
            ({"value": "age"}, self.basic_table.c.age),
            # Conditions
            (
                {
                    "value": "age",
                    "condition": {"field": "last", "in": ["Jones", "Punjabi"]},
                },
                case(
                    [
                        (
                            self.basic_table.c.last.in_(["Jones", "Punjabi"]),
                            self.basic_table.c.age,
                        )
                    ]
                ),
            ),
            # # Date trunc
            # (
            #     {"value": "age", "aggregation": "month"},
            #     func.date_trunc("month", self.basic_table.c.age),
            # ),
            # (
            #     {"value": "age", "aggregation": "week"},
            #     func.date_trunc("week", self.basic_table.c.age),
            # ),
            # (
            #     {"value": "age", "aggregation": "year"},
            #     func.date_trunc("year", self.basic_table.c.age),
            # ),
            # (
            #     {"value": "age", "aggregation": "age"},
            #     func.date_part("year", func.age(self.basic_table.c.age)),
            # ),
            # # Conditions
            # (
            #     {
            #         "value": "age",
            #         "condition": {"field": "last", "in": ["Jones", "Punjabi"]},
            #     },
            #     func.sum(case([(self.basic_table.c.last.in_(["Jones", "Punjabi"]), self.basic_table.c.age)])),
            # ),
        ]
        for input_field, expected_result in data:
            result = parse_field(
                input_field, selectable=self.basic_table, aggregated=False
            )
            self.assertEqual(str(result), str(expected_result))

    def test_weird_field_string_definitions(self):
        data = [
            ("first+", self.basic_table.c.first),
            ("first-", self.basic_table.c.first),
            ("fir st-", self.basic_table.c.first),
            ("fir st", self.basic_table.c.first),
            ("first+last-", "foo.first || foo.last"),
            ("fir st*", self.basic_table.c.first),
            (
                "first/last-",
                self.basic_table.c.first
                / (
                    func.coalesce(cast(self.basic_table.c.last, Float), 0.0)
                    + SAFE_DIVISON_EPSILON
                ),
            ),
        ]
        for input_field, expected_result in data:
            result = parse_field(
                input_field, selectable=self.basic_table, aggregated=False
            )
            self.assertEqual(str(result), str(expected_result))

    def test_bad_field_definitions(self):
        bad_data = [
            {},
            [],
            ["abb"],
            ["age"],
            {"value": ["age"]},
            {"condition": ["age"]},
            {"condition": "foo"},
            {"condition": []},
        ]
        for input_field in bad_data:
            with self.assertRaises(BadIngredient):
                parse_field(input_field, self.basic_table)

    def test_field_with_invalid_column(self):
        bad_data = ["abb", {"value": "abb"}]
        for input_field in bad_data:
            with self.assertRaises(InvalidColumnError):
                field = parse_field(input_field, self.basic_table)
