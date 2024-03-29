from json import dumps
from copy import copy
from faker import Faker
from sqlalchemy import func, and_, or_
from sureberus.errors import BadType, SureError

from recipe import BadRecipe, Dimension, Metric, Recipe, Shelf
from recipe.extensions import (
    Anonymize,
    AutomaticFilters,
    BlendRecipe,
    CompareRecipe,
    Paginate,
    PaginateInline,
    PaginateCountOver,
    RecipeExtension,
    handle_directives,
    is_compound_filter,
)
from recipe.utils import generate_faker_seed, recipe_arg
from tests.test_base import RecipeTestCase


def convert_to_json_encoded(d: dict) -> dict:
    newd = {}
    for k, v in d.items():
        if "," in k and isinstance(v, list):
            v = [itm if isinstance(itm, str) else dumps(itm) for itm in v]
        newd[k] = v
    return newd


class DummyExtension(RecipeExtension):
    recipe_schema = {"a": {"type": "string"}, "a_int": {"type": "integer"}}

    def __init__(self, recipe):
        super().__init__(recipe)
        self.value = None

    @recipe_arg()
    def from_config(self, obj):
        # a_int is in schema but is not handled by a directive
        handle_directives(obj, {"a": self.a})

    @recipe_arg()
    def a(self, value):
        self.value = value

    @recipe_arg()
    def a_int(self, value):
        self.value = str(value * 2)


class ExtensionAttributesTestCase(RecipeTestCase):
    """Test that extensions expose attributes that are available for chaining on recipe"""

    def test_call_extension_method(self):
        self.extension_classes = []
        recipe = self.recipe().metrics("age").dimensions("first")

        with self.assertRaises(AttributeError):
            value = recipe.a("foo")

        with self.assertRaises(AttributeError):
            recipe.b("moo")

        self.extension_classes = [DummyExtension]
        recipe = self.recipe().metrics("age").dimensions("first").a("foo")
        self.assertEqual(recipe.recipe_extensions[0].value, "foo")

        with self.assertRaises(AttributeError):
            recipe.b("moo")

    def test_call_extension_method_from_config(self):
        self.extension_classes = []
        # Without an extension, unknown properties are ignored
        recipe = self.recipe_from_config(
            {"metrics": ["age"], "dimensions": ["first"], "a": 22}
        )

        # But we can't use them on the Recipe object
        with self.assertRaises(AttributeError):
            value = recipe.a("foo")

        with self.assertRaises(AttributeError):
            recipe.b("moo")

        # When we use the extension, we will validate the property
        self.extension_classes = [DummyExtension]
        recipe = self.recipe_from_config(
            {"metrics": ["age"], "dimensions": ["first"], "a": "foo"}
        )
        self.assertEqual(recipe.recipe_extensions[0].value, "foo")

        # The property must be a string per DummyExtension.recipe_schema
        with self.assertRaises(BadType):
            recipe = self.recipe_from_config(
                {"metrics": ["age"], "dimensions": ["first"], "a": 22}
            )

        with self.assertRaises(AttributeError):
            recipe.b()

        # Pass an unhandled config option.
        with self.assertRaises(BadRecipe):
            r = self.recipe_from_config(
                {"metrics": ["age"], "dimensions": ["first"], "a_int": 2}
            )


class SimpleExtensionTestCase(RecipeTestCase):
    """Test a simple extension that always adds a filter"""

    def setUp(self):
        super().setUp()

        class AddFilter(RecipeExtension):
            """A simple extension that adds a filter to every query."""

            bt = self.basic_table

            def add_ingredients(self):
                self.recipe.filters(self.bt.c.first > 2)

        self.extension_classes = [AddFilter]

    def test_add_filter_from_config(self):
        recipe = self.recipe_from_config({"metrics": ["age"], "dimensions": ["first"]})

    def test_add_filter(self):
        recipe = self.recipe().metrics("age").dimensions("first")
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS first,
            sum(foo.age) AS age
        FROM foo
        WHERE foo.first > 2
        GROUP BY first""",
        )

        # Building the recipe from config gives the same results
        recipe_from_config = self.recipe_from_config(
            {"metrics": ["age"], "dimensions": ["first"]}
        )
        self.assertEqual(recipe.to_sql(), recipe_from_config.to_sql())
        self.assertEqual(recipe.dataset.csv, recipe_from_config.dataset.csv)


class AutomaticFiltersTestCase(RecipeTestCase):
    """The AutomaticFilters extension."""

    extension_classes = [AutomaticFilters]

    def test_from_config(self):
        """Check the internal state of an extension after configuration"""
        for recipe in self.recipe_list(
            (
                self.recipe()
                .metrics("age")
                .dimensions("first")
                .automatic_filters({"first": ["foo"]})
                .include_automatic_filter_keys("foo", "moo")
                .exclude_automatic_filter_keys("foo", "you")
                .apply_automatic_filters(False)
            ),
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": ["foo"]},
                "include_automatic_filter_keys": ["foo", "moo"],
                "exclude_automatic_filter_keys": ["foo", "you"],
                "apply_automatic_filters": False,
            },
        ):
            ext = recipe.recipe_extensions[0]
            self.assertEqual(ext._automatic_filters, [{"first": ["foo"]}])
            self.assertEqual(ext.include_keys, ("foo", "moo"))
            self.assertEqual(ext.exclude_keys, ("foo", "you"))
            self.assertFalse(ext.apply)

    def test_multiple_filter_dicts(self):
        """Check the internal state of an extension after configuration"""
        for recipe in self.recipe_list(
            (
                self.recipe()
                .metrics("age")
                .dimensions("first")
                .automatic_filters({"first": ["foo"]})
                .automatic_filters({"second": ["moo"]})
                .include_automatic_filter_keys("foo", "moo")
                .exclude_automatic_filter_keys("foo", "you")
                .apply_automatic_filters(False)
            ),
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": [{"first": ["foo"]}, {"second": ["moo"]}],
                "include_automatic_filter_keys": ["foo", "moo"],
                "exclude_automatic_filter_keys": ["foo", "you"],
                "apply_automatic_filters": False,
            },
        ):
            ext = recipe.recipe_extensions[0]
            self.assertEqual(
                ext._automatic_filters, [{"first": ["foo"]}, {"second": ["moo"]}]
            )
            self.assertEqual(ext.include_keys, ("foo", "moo"))
            self.assertEqual(ext.exclude_keys, ("foo", "you"))
            self.assertFalse(ext.apply)

    def test_recipe_schema(self):
        """From config values are validated"""
        base_config = {"metrics": ["age"], "dimensions": ["first"]}
        valid_configs = [
            {"automatic_filters": {"first": ["foo"], "last": ["cow", "pig", "fruit"]}},
            {"automatic_filters": {"first": ["foo"]}},
            {"include_automatic_filter_keys": ["potato", "avocado"]},
            {"exclude_automatic_filter_keys": ["potato"]},
            {"exclude_automatic_filter_keys": []},
            {"apply_automatic_filters": True},
            {"strict_automatic_filters": True},
        ]
        for extra_config in valid_configs:
            config = copy(base_config)
            config.update(extra_config)
            # We can construct and run the recipe
            recipe = self.recipe_from_config(config)
            recipe.all()
            self.assertRecipeSQLContains(recipe, "age")

        invalid_configs = [
            # Keys must be strings
            {"automatic_filters": {2: ["foo"]}},
            # Must be a dict
            {"automatic_filters": 2},
            # Values must be strings
            {"include_automatic_filter_keys": ["potato", 2]},
            # Values must be lists of strings
            {"exclude_automatic_filter_keys": "potato"},
            # Values must be lists of strings
            {"apply_automatic_filters": "TRUE"},
        ]
        for extra_config in invalid_configs:
            config = copy(base_config)
            config.update(extra_config)
            with self.assertRaises(SureError):
                recipe = self.recipe_from_config(config)

    def test_builder_pattern(self):
        """We can chain directives together, returning recipe"""
        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("first")
            .apply_automatic_filters(False)
        )

        self.assertFalse(recipe.recipe_extensions[0].apply)

        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("first")
            .include_automatic_filter_keys("first")
        )
        self.assertEqual(recipe.recipe_extensions[0].include_keys, ("first",))

        # Test chaining
        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("first")
            .include_automatic_filter_keys("first")
            .exclude_automatic_filter_keys("last")
        )
        self.assertEqual(recipe.recipe_extensions[0].include_keys, ("first",))
        self.assertEqual(recipe.recipe_extensions[0].exclude_keys, ("last",))

    def test_apply(self):
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "apply_automatic_filters": False,
            }
        ):
            self.assertFalse(recipe.recipe_extensions[0].apply)
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
                    sum(foo.age) AS age
                FROM foo
                GROUP BY first""",
            )

        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "apply_automatic_filters": True,
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
                    sum(foo.age) AS age
                FROM foo
                GROUP BY first""",
            )

    def test_automatic_filters(self):
        """Automatic filters must be a dict"""
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": ["foo"]},
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
                    sum(foo.age) AS age
                FROM foo
                WHERE foo.first IN ('foo')
                GROUP BY first""",
            )

        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": [None]},
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
                sum(foo.age) AS age
            FROM foo
            WHERE foo.first IS NULL
            GROUP BY first""",
            )

    def test_multiple_automatic_filters(self):
        """Automatic filters can be passed multiple times and all will apply"""
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": [{"first": ["foo"]}, {"last__gt": "x"}],
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            self.assertRecipeSQLContains(recipe, "foo.first IN ('foo')")
            self.assertRecipeSQLContains(recipe, "foo.last > 'x'")

        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("first")
            .automatic_filters({"first": ["foo"]})
            .automatic_filters({"last__gt": "t"})
        )
        self.assertTrue(recipe.recipe_extensions[0].apply)
        self.assertRecipeSQLContains(recipe, "foo.first IN ('foo')")
        self.assertRecipeSQLContains(recipe, "foo.last > 't'")

    def test_multiple_automatic_filters_with_exclude(self):
        """Automatic filters can be passed multiple times and all will apply"""
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": [{"first": ["foo"]}, {"last__gt": "x"}],
                "exclude_automatic_filter_keys": ["last"],
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            self.assertRecipeSQLContains(recipe, "foo.first IN ('foo')")
            self.assertRecipeSQLNotContains(recipe, "foo.last > 'x'")

        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("first")
            .automatic_filters({"first": ["foo"]})
            .automatic_filters({"last__gt": "t"})
            .exclude_automatic_filter_keys("last")
        )
        self.assertTrue(recipe.recipe_extensions[0].apply)
        self.assertRecipeSQLContains(recipe, "foo.first IN ('foo')")
        self.assertRecipeSQLNotContains(recipe, "foo.last > 't'")

    def test_automatic_filters_with_unknown_dim(self):
        """Test filters built using an unknown dimension"""
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": ["foo"], "potato": ["pancake"]},
            },
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": ["foo"], "potato__in": ["pancake"]},
            },
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": [None], "potato": "pancake"},
            },
            # Compound filters with unknown key
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {
                    "first,potato": [["foo", "moo"], ["chicken", "cluck"]]
                },
            },
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "strict_automatic_filters": True,
                "automatic_filters": {
                    "potato,first": [["foo", "moo"], ["chicken", "cluck"]]
                },
            },
        ):
            with self.assertRaises(BadRecipe):
                recipe.to_sql()

    def test_automatic_filters_with_unknown_dim_nostrict(self):
        """Test filters built using an unknown dimension"""
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "strict_automatic_filters": False,
                "automatic_filters": {"first": ["foo"], "potato": ["pancake"]},
            },
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "strict_automatic_filters": False,
                "automatic_filters": {"first": ["foo"], "potato__in": ["pancake"]},
            },
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "strict_automatic_filters": False,
                "automatic_filters": {"first": [None], "potato": "pancake"},
            },
            # Compound filters with unknown key
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "strict_automatic_filters": False,
                "automatic_filters": {
                    "first,potato": [["foo", "moo"], ["chicken", "cluck"]]
                },
            },
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "strict_automatic_filters": False,
                "automatic_filters": {
                    "potato,first": [["foo", "moo"], ["chicken", "cluck"]]
                },
            },
        ):
            # SQL gets generated filtering on foo.first, the potato ingredient is ignored
            self.assertRecipeSQLContains(recipe, "WHERE foo.first")
            self.assertRecipeSQLNotContains(recipe, "potato")

    def test_apply_automatic_filters(self):
        recipe = (
            self.recipe()
            .metrics("age")
            .dimensions("first")
            .automatic_filters({"first": ["foo"]})
            .apply_automatic_filters(False)
        )
        self.assertTrue("WHERE" not in recipe.to_sql().upper())

    def test_include_exclude_keys(self):
        # Include and exclude keys that don't appear in automatic filters
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": ["foo"]},
                "include_automatic_filter_keys": ["foo"],
            },
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": ["foo"]},
                "exclude_automatic_filter_keys": ["first"],
            },
        ):
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
                    sum(foo.age) AS age
                FROM foo
                GROUP BY first""",
            )

        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": ["foo"]},
                "include_automatic_filter_keys": ["foo", "first"],
            },
            # Excluding irrelevant keys
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first": ["foo"]},
                "exclude_automatic_filter_keys": ["foo"],
            },
        ):
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
                    sum(foo.age) AS age
                FROM foo
                WHERE foo.first IN ('foo')
                GROUP BY first""",
            )

    def test_is_compound_filter(self):
        valid_compound_keys = ["a,b", "a,b,c", "a,b,,c", ",a,bb,aac", ","]
        for k in valid_compound_keys:
            self.assertTrue(is_compound_filter(k))

        # These are not compound filters
        invalid_compound_keys = ["ab", "", "ab__notin"]
        for k in invalid_compound_keys:
            self.assertFalse(is_compound_filter(k))

    def test_operators_and_compound_filters(self):
        """Test operators and compound filters. Filter values may be json encoded"""
        # Testing operators
        values = (
            ({"first__notin": ["foo"]}, "foo.first NOT IN ('foo')"),
            ({"first__between": ["foo", "moo"]}, "foo.first BETWEEN 'foo' AND 'moo'"),
            # Case doesn't matter
            ({"first__NOTIN": ["foo"]}, "foo.first NOT IN ('foo')"),
            ({"first__betWEEN": ["foo", "moo"]}, "foo.first BETWEEN 'foo' AND 'moo'"),
            ({"first__lt": "moo"}, "foo.first < 'moo'"),
            (
                {"first,last": [["foo", "moo"]]},
                "foo.first = 'foo'\n  AND foo.last = 'moo'",
            ),
            (
                {"first,last": [["foo", "moo"], ["chicken", "cluck"]]},
                """foo.first = 'foo'
  AND foo.last = 'moo'
  OR foo.first = 'chicken'
  AND foo.last = 'cluck'""",
            ),
            # Unbalanced compound filters
            (
                {"first,last": [["foo", "moo"], ["chicken"]]},
                """foo.first = 'foo'
  AND foo.last = 'moo'
  OR foo.first = 'chicken'""",
            ),
            # Compound filters with operators
            (
                {"first,last__like": [["cow", "moo%"], ["chicken", "cluck%"]]},
                """foo.first = 'cow'
  AND foo.last LIKE 'moo%'
  OR foo.first = 'chicken'
  AND foo.last LIKE 'cluck%'""",
            ),
            # Compound filters may be json encoded
            (
                {"first,last": ['["foo", "moo"]']},
                """foo.first = 'foo'
  AND foo.last = 'moo'""",
            ),
            # Compound filters, json encoded, multiple items
            (
                {"first,last": ['["foo", "moo"]', '["chicken", "cluck"]']},
                """foo.first = 'foo'
  AND foo.last = 'moo'
  OR foo.first = 'chicken'
  AND foo.last = 'cluck'""",
            ),
            (
                {"first__in,first__notin": ['[["foo"], ["moo","cow"]]']},
                """foo.first IN ('foo')
  AND (foo.first NOT IN ('cow',
                        'moo'))""",
            ),
        )
        for af, expected_sql in values:
            # json encode all the values
            json_encoded_af = convert_to_json_encoded(af)
            for recipe in self.recipe_list(
                {
                    "metrics": ["age"],
                    "dimensions": ["first"],
                    "automatic_filters": af,
                    "exclude_automatic_filter_keys": ["foo"],
                },
                {
                    "metrics": ["age"],
                    "dimensions": ["first"],
                    "automatic_filters": json_encoded_af,
                    "exclude_automatic_filter_keys": ["foo"],
                },
            ):
                print(recipe.to_sql())
                self.assertRecipeSQLContains(recipe, expected_sql)

    def test_no_where_filters(self):
        # Testing filters that don't add a where
        values = [
            {},
            # Excluded by automatic filter keys
            {"foo": ["22"]},
            {"foo": []},
            # Excluded by automatic filter keys
            {"first": ["hi", "there"]},
            # Compound filters with no values are excluded
            {"first,last": [[]]},
            {"first,last": [[], []]},
        ]
        for af in values:
            for recipe in self.recipe_list(
                {
                    "metrics": ["age"],
                    "dimensions": ["first"],
                    "automatic_filters": af,
                    "exclude_automatic_filter_keys": ["foo", "first"],
                }
            ):
                self.assertRecipeSQLNotContains(recipe, "WHERE")

    def test_exclude_all_filters(self):
        # Testing filters that exclude everything
        values = [{"first": []}]
        for af in values:
            for recipe in self.recipe_list(
                {
                    "metrics": ["age"],
                    "dimensions": ["first"],
                    "automatic_filters": af,
                    "exclude_automatic_filter_keys": ["foo"],
                }
            ):
                print(recipe.to_sql())
                self.assertRecipeSQLContains(recipe, "WHERE 1!=1")

    def test_invalid_compound_filters(self):
        bad_compound_filters = [
            # Values must be either lists or json encoded lists
            (
                {"first,last": ['"foo"']},
                "Compound filter values must be json encoded lists",
            ),
            # Value must be valid json
            ({"first,last": ['["foo"']}, "Compound filter values must be valid json"),
            # Keys must be comma delimited valid dimensions
            ({"first,potato": ['["moo", "cow"]']}, "potato doesn't exist on the shelf"),
        ]

        for bad_filter, error_msg in bad_compound_filters:
            with self.assertRaises((ValueError, BadRecipe)) as cm:
                recipe = self.recipe_from_config(
                    {
                        "metrics": ["age"],
                        "dimensions": ["first"],
                        "automatic_filters": bad_filter,
                        "exclude_automatic_filter_keys": ["foo"],
                    }
                )
                recipe.all()
            self.assertEqual(str(cm.exception), error_msg)

    def test_invalid_compound_filters_nostrict(self):
        bad_compound_filters = [
            # Values must be either lists or json encoded lists
            (
                {"first,last": ['"foo"']},
                "Compound filter values must be json encoded lists",
            ),
            # Value must be valid json
            ({"first,last": ['["foo"']}, "Compound filter values must be valid json"),
            # Keys must be comma delimited valid dimensions
            # ({"first,potato": ['["moo", "cow"]']}, "potato doesn't exist on the shelf")
        ]

        for bad_filter, error_msg in bad_compound_filters:
            with self.assertRaises((ValueError, BadRecipe)) as cm:
                recipe = self.recipe_from_config(
                    {
                        "metrics": ["age"],
                        "dimensions": ["first"],
                        "automatic_filters": bad_filter,
                        "strict_automatic_filters": False,
                        "exclude_automatic_filter_keys": ["foo"],
                    }
                )
                recipe.all()
            self.assertEqual(str(cm.exception), error_msg)

        recipe = self.recipe_from_config(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"first,potato": ['["moo", "cow"]']},
                "strict_automatic_filters": False,
                "exclude_automatic_filter_keys": ["foo"],
            }
        )
        # Same as {"first": ["moo"]}
        self.assertRecipeSQL(
            recipe,
            """
SELECT foo.first AS first,
    sum(foo.age) AS age
FROM foo
WHERE foo.first = 'moo'
GROUP BY first
            """,
        )

    def test_invalid_operators(self):
        """Invalid operators raise an exception"""
        # Testing operators
        values = [{"last__mike": "moo"}]
        for af in values:
            for recipe in self.recipe_list(
                {
                    "metrics": ["age"],
                    "dimensions": ["first"],
                    "automatic_filters": af,
                    "strict_automatic_filters": True,
                    "exclude_automatic_filter_keys": ["foo"],
                }
            ):
                with self.assertRaises(BadRecipe):
                    recipe.all()

        # If we add this to the shelf, it works.
        newshelf = Shelf(
            {
                "first": Dimension(self.basic_table.c.first),
                "last": Dimension(self.basic_table.c.last),
                "firstlast": Dimension(
                    self.basic_table.c.last, id_expression=self.basic_table.c.first
                ),
                "age": Metric(func.sum(self.basic_table.c.age)),
                "last__mike": Dimension(self.basic_table.c.last),
            }
        )

        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"last__mike": "moo"},
            },
            shelf=newshelf,
        ):
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
        sum(foo.age) AS age
    FROM foo
    WHERE foo.last = 'moo'
    GROUP BY first""",
            )

        # We can chain a valid operator to an ingredient that has a __
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": {"last__mike__like": "moo"},
            },
            shelf=newshelf,
        ):
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.last LIKE 'moo'
GROUP BY first""",
            )


class AnonymizeTestCase(RecipeTestCase):
    extension_classes = [Anonymize]

    def setUp(self):
        super().setUp()
        self.shelf = Shelf(
            {
                "first": Dimension(self.basic_table.c.first),
                "firstanon": Dimension(
                    self.basic_table.c.first, anonymizer="{fake:name}"
                ),
                "last": Dimension(
                    self.basic_table.c.last, anonymizer=lambda value: value[::-1]
                ),
                "age": Metric(func.sum(self.basic_table.c.age)),
            }
        )

    def test_from_config(self):
        """Check the internal state of an extension after configuration and regular construction"""
        for recipe in self.recipe_list(
            (self.recipe().metrics("age").dimensions("first").anonymize(True)),
            {"metrics": ["age"], "dimensions": ["first"], "anonymize": True},
        ):
            ext = recipe.recipe_extensions[0]
            self.assertTrue(ext._anonymize)

    def test_recipe_schema(self):
        """From config values are validated"""
        base_config = {"metrics": ["age"], "dimensions": ["first"]}
        valid_configs = [{"anonymize": True}, {"anonymize": False}]
        for extra_config in valid_configs:
            config = copy(base_config)
            config.update(extra_config)
            # We can construct and run the recipe
            recipe = self.recipe_from_config(config)
            recipe.all()

        invalid_configs = [{"anonymize": "TRUE"}, {"anonymize": 0}]
        for extra_config in invalid_configs:
            config = copy(base_config)
            config.update(extra_config)
            with self.assertRaises(SureError):
                recipe = self.recipe_from_config(config)

    def test_anonymize_with_anonymizer(self):
        """Anonymize requires ingredients to have an anonymizer"""
        recipe = self.recipe_from_config(
            {
                "metrics": ["age"],
                "dimensions": ["last"],
                "order_by": ["last"],
                "anonymize": False,
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            last,age,last_id
            fred,10,fred
            there,5,there
            """,
        )

        recipe = self.recipe_from_config(
            {
                "metrics": ["age"],
                "dimensions": ["last"],
                "order_by": ["last"],
                "anonymize": True,
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            last_raw,age,last,last_id
            fred,10,derf,fred
            there,5,ereht,there
            """,
        )

    def test_anonymize_with_faker_anonymizer(self):
        """Anonymize requires ingredients to have an anonymizer"""
        recipe = self.recipe_from_config(
            {
                "metrics": ["age"],
                "dimensions": ["firstanon"],
                "order_by": ["firstanon"],
                "anonymize": False,
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            firstanon,age,firstanon_id
            hi,15,hi
            """,
        )

        recipe = self.recipe_from_config(
            {
                "metrics": ["age"],
                "dimensions": ["firstanon"],
                "order_by": ["firstanon"],
                "anonymize": True,
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            firstanon_raw,age,firstanon,firstanon_id
            hi,15,Grant Hernandez,hi
            """,
        )

        # Faker values are deterministic
        fake = Faker(locale="en_US")
        fake.seed_instance(generate_faker_seed("hi"))
        fake_value = fake.name()
        self.assertEqual(fake_value, "Grant Hernandez")

    def test_anonymize_without_anonymizer(self):
        """If the dimension doesn't have an anonymizer, there is no change"""
        for anonymize_it in (True, False):
            recipe = (
                self.recipe()
                .metrics("age")
                .dimensions("first")
                .order_by("first")
                .anonymize(anonymize_it)
            )
            self.assertRecipeCSV(
                recipe,
                """
                first,age,first_id
                hi,15,hi
                """,
            )


class PaginateTestCase(RecipeTestCase):
    """Test Pagination

    Developer's Note: For pagination we are testing csv results
    rather than generated sql because we want to use the same tests
    to test the Paginate and the PaginateInline which generate different
    SQL.
    """

    extension_classes = [PaginateInline]

    def setUp(self):
        super().setUp()
        self.shelf = self.census_shelf

    def test_from_config(self):
        """Check the internal state of an extension after configuration"""
        for recipe in self.recipe_list(
            (
                self.recipe()
                .metrics("pop2000")
                .dimensions("state", "sex")
                .pagination_page_size(10)
                .pagination_page(1)
                .pagination_q("T%")
                .pagination_search_keys("state", "sex")
                .pagination_order_by("-sex")
            ),
            {
                "metrics": ["pop2000"],
                "dimensions": ["state", "sex"],
                "pagination_page_size": 10,
                "pagination_page": 1,
                "pagination_q": "T%",
                "pagination_search_keys": ["state", "sex"],
                "pagination_order_by": ["-sex"],
            },
        ):
            ext = recipe.recipe_extensions[0]
            self.assertEqual(ext._apply_pagination, True)
            self.assertEqual(ext._pagination_q, "T%")
            self.assertEqual(ext._paginate_search_keys, ("state", "sex"))
            self.assertEqual(ext._pagination_page_size, 10)
            self.assertEqual(ext._pagination_page, 1)
            self.assertEqual(ext._pagination_order_by, ("-sex",))
            self.assertEqual(ext._validated_pagination, None)

            # After the recipe runs, validated pagination is available
            self.assertRecipeCSV(
                recipe,
                """
                sex,state,pop2000,sex_id,state_id
                M,Tennessee,2761277,M,Tennessee
                F,Tennessee,2923953,F,Tennessee
                """,
                ignore_columns=["recipe_total_count"],
            )
            self.assertEqual(
                recipe.validated_pagination(),
                {"requestedPage": 1, "page": 1, "pageSize": 10, "totalItems": 2},
            )

    def test_recipe_schema(self):
        """From config values are validated"""
        base_config = {"metrics": ["pop2000"], "dimensions": ["state"]}
        valid_configs = [
            {"apply_pagination": True},
            {"apply_pagination_filters": False},
            {"pagination_order_by": ["-state"]},
            {"pagination_order_by": ["-state"]},
            {"pagination_order_by": ["-state", "pop2000"]},
            {"pagination_q": "T%"},
            {"pagination_q": "T_stad_"},
            {"pagination_search_keys": ["state"]},
            {"pagination_page_size": 1000},
            {"pagination_page_size": 0},
            {"pagination_page": -1},
            {"pagination_page": 100},
        ]
        for extra_config in valid_configs:
            config = copy(base_config)
            config.update(extra_config)
            # We can construct and run the recipe
            recipe = self.recipe_from_config(config)
            recipe.all()
            self.assertRecipeSQLContains(recipe, "state")

        invalid_configs = [
            {"apply_pagination": 1},
            {"apply_pagination_filters": [False]},
            {"pagination_order_by": [1]},
            {"pagination_order_by": {"name": "fred"}},
            {"pagination_order_by": None},
            {"pagination_q": ["T%"]},
            {"pagination_q": 23},
            {"pagination_search_keys": [25]},
            {"pagination_page_size": "a"},
            {"pagination_page_size": []},
            {"pagination_page_size": -5},
            {"pagination_page": ["foo"]},
            {"pagination_page": 900.0},
        ]
        for extra_config in invalid_configs:
            config = copy(base_config)
            config.update(extra_config)
            with self.assertRaises(SureError):
                recipe = self.recipe_from_config(config)

    def test_no_pagination(self):
        """Pagination is not on until configured"""
        for recipe in self.recipe_list(
            {"metrics": ["pop2000"], "dimensions": ["state"]}
        ):
            self.assertTrue("LIMIT" not in recipe.to_sql())

    def test_pagination(self):
        """If pagination page size is configured, pagination is applied to results"""
        for recipe in self.recipe_list(
            {"metrics": ["pop2000"], "dimensions": ["age"], "pagination_page_size": 10}
        ):
            self.assertRecipeSQLContains(recipe, "LIMIT 10")
            self.assertRecipeSQLContains(recipe, "OFFSET 0")
            self.assertEqual(
                recipe.validated_pagination(),
                {"page": 1, "pageSize": 10, "requestedPage": 1, "totalItems": 86},
            )

            # Let's go to the second page
            recipe = recipe.pagination_page(2)
            self.assertRecipeSQLContains(recipe, "LIMIT 10")
            self.assertRecipeSQLContains(recipe, "OFFSET 10")

            self.assertEqual(
                recipe.validated_pagination(),
                {"page": 2, "pageSize": 10, "requestedPage": 2, "totalItems": 86},
            )

            # Let's go to an impossible page
            recipe = recipe.pagination_page(9)
            self.assertRecipeSQLContains(recipe, "LIMIT 10")
            self.assertRecipeSQLContains(recipe, "OFFSET 80")

            # page is clamped to the real value
            self.assertEqual(
                recipe.validated_pagination(),
                {"page": 9, "pageSize": 10, "requestedPage": 9, "totalItems": 86},
            )

            recipe = recipe.pagination_page(-1)
            self.assertRecipeSQLContains(recipe, "LIMIT 10")
            self.assertRecipeSQLContains(recipe, "OFFSET 0")

            self.assertEqual(
                recipe.validated_pagination(),
                {"page": 1, "pageSize": 10, "requestedPage": 1, "totalItems": 86},
            )

            # What if there's no data
            recipe = (
                recipe.filters("filter_all").pagination_page(1).pagination_page_size(5)
            )
            self.assertRecipeSQLContains(recipe, "WHERE 0 = 1")
            self.assertRecipeSQLContains(recipe, "LIMIT 5")
            self.assertRecipeSQLContains(recipe, "OFFSET 0")
            self.assertEqual(
                recipe.validated_pagination(),
                {"page": 1, "pageSize": 5, "requestedPage": 1, "totalItems": 0},
            )

    def test_apply_pagination(self):
        for recipe in self.recipe_list(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state"],
                "pagination_page_size": 10,
                "apply_pagination": False,
            }
        ):
            self.assertRecipeSQLNotContains(recipe, "LIMIT")
            self.assertRecipeSQLNotContains(recipe, "OFFSET")

    def test_pagination_order_by(self):
        """Pagination requires ordering"""

        for recipe in self.recipe_list(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state"],
                "pagination_page_size": 10,
                "pagination_order_by": ["-state"],
            }
        ):
            self.assertRecipeSQLContains(recipe, "ORDER BY state DESC")
            self.assertRecipeSQLContains(recipe, "LIMIT 10")

    def test_pagination_default_order_by(self):
        # Default order by applies a pagination

        for recipe in self.recipe_list(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state"],
                "pagination_page_size": 10,
                "pagination_default_order_by": ["-pop2000"],
            }
        ):
            self.assertRecipeSQLContains(recipe, "ORDER BY pop2000 DESC")
            # Default ordering is not used when the recipe already
            # has an ordering
            recipe = recipe.order_by("state")
            self.assertRecipeSQLNotContains(recipe, "ORDER BY pop2000 DESC")
            self.assertRecipeSQLContains(recipe, "ORDER BY state")

        # Default ordering is not used when the recipe
        # has a explicit pagination_order_by
        # has an ordering
        for recipe in self.recipe_list(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state"],
                "order_by": ["state"],
                "pagination_page_size": 10,
                "pagination_default_order_by": ["-pop2000"],
            }
        ):
            self.assertRecipeSQLContains(recipe, "ORDER BY state")

    def test_pagination_q(self):
        recipe = self.recipe_from_config(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state"],
                "pagination_page_size": 10,
                "pagination_q": "T%",
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            state,pop2000,state_id
            Tennessee,5685230,Tennessee
            """,
        )

    def test_pagination_q_idvalue(self):
        """Pagination queries use the value of an id value dimension"""
        recipe = self.recipe_from_config(
            {
                "metrics": ["pop2000"],
                "dimensions": ["idvalue_state"],
                "pagination_page_size": 10,
                "pagination_q": "State:T%",
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            idvalue_state_id,idvalue_state,pop2000
            Tennessee,State:Tennessee,5685230
            """,
        )

        self.assertRecipeCSV(
            recipe,
            """
            idvalue_state_id,idvalue_state,pop2000
            Tennessee,State:Tennessee,5685230
            """,
        )

    def test_apply_pagination_filters(self):
        """apply_pagination_filters False will disable adding search"""
        recipe = self.recipe_from_config(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state"],
                "pagination_page_size": 10,
                "pagination_q": "T%",
                "apply_pagination_filters": False,
            }
        )

    def test_pagination_search_keys(self):
        recipe = self.recipe_from_config(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state"],
                "pagination_page_size": 10,
                "pagination_q": "M",
                "pagination_search_keys": ["sex", "state"],
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            state,pop2000,state_id
            Tennessee,2761277,Tennessee
            Vermont,298532,Vermont
            """,
        )

    def test_all(self):
        """Test all pagination options together"""
        recipe = self.recipe_from_config(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state", "sex", "age"],
                "pagination_page_size": 10,
                "pagination_page": 5,
                "pagination_q": "T%",
                "pagination_search_keys": ["state", "sex"],
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            age,sex,state,pop2000,age_id,sex_id,state_id
            40,F,Tennessee,47199,40,F,Tennessee
            41,F,Tennessee,45660,41,F,Tennessee
            42,F,Tennessee,45959,42,F,Tennessee
            43,F,Tennessee,46308,43,F,Tennessee
            44,F,Tennessee,44914,44,F,Tennessee
            45,F,Tennessee,45282,45,F,Tennessee
            46,F,Tennessee,43943,46,F,Tennessee
            47,F,Tennessee,42004,47,F,Tennessee
            48,F,Tennessee,41435,48,F,Tennessee
            49,F,Tennessee,39967,49,F,Tennessee
            """,
            ignore_columns=["recipe_total_count"],
        )


class PaginateInlineTestCase(PaginateTestCase):
    """Run all the paginate tests with a different paginator

    PaginateInline will add a "recipe_total_count" column to each returned row.
    Be sure to ignore this when evaluating results.
    """

    extension_classes = [PaginateInline]

    def assertRecipeCSV(
        self, recipe: Recipe, csv_text: str, ignore_columns=["recipe_total_count"]
    ):
        super().assertRecipeCSV(recipe, csv_text, ignore_columns=ignore_columns)

    def test_pagination_q_idvalue(self):
        """Pagination queries use the value of an id value dimension"""
        recipe = self.recipe_from_config(
            {
                "metrics": ["pop2000"],
                "dimensions": ["idvalue_state"],
                "pagination_page_size": 10,
                "pagination_q": "State:T%",
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            idvalue_state_id,idvalue_state,pop2000
            Tennessee,State:Tennessee,5685230
            """,
        )

        self.assertRecipeCSV(
            recipe,
            """
            idvalue_state_id,idvalue_state,pop2000
            Tennessee,State:Tennessee,5685230
            """,
        )


class PaginateCountOverTestCase(PaginateTestCase):
    """Run all the paginate tests with a different paginator

    PaginateInline will add a "_total_count" column to each returned row.
    Be sure to ignore this when evaluating results.
    """

    extension_classes = [PaginateCountOver]

    def assertRecipeCSV(
        self, recipe: Recipe, csv_text: str, ignore_columns=["recipe_total_count"]
    ):
        super().assertRecipeCSV(recipe, csv_text, ignore_columns=ignore_columns)

    def test_pagination_q_idvalue(self):
        """Pagination queries use the value of an id value dimension"""
        recipe = self.recipe_from_config(
            {
                "metrics": ["pop2000"],
                "dimensions": ["idvalue_state"],
                "pagination_page_size": 10,
                "pagination_q": "State:T%",
            }
        )
        self.assertRecipeCSV(
            recipe,
            """
            idvalue_state_id,idvalue_state,pop2000
            Tennessee,State:Tennessee,5685230
            """,
        )

        self.assertRecipeCSV(
            recipe,
            """
            idvalue_state_id,idvalue_state,pop2000
            Tennessee,State:Tennessee,5685230
            """,
        )

    def test_count_over_sql(self):
        """Test all pagination options together"""
        recipe = self.recipe_from_config(
            {
                "metrics": ["pop2000"],
                "dimensions": ["state", "sex", "age"],
                "pagination_page_size": 10,
                "pagination_page": 5,
                "pagination_q": "T%",
                "pagination_search_keys": ["state", "sex"],
            }
        )
        self.assertRecipeSQL(
            recipe,
            """
            SELECT census.age AS age,
                census.sex AS sex,
                census.state AS state,
                sum(census.pop2000) AS pop2000,
                count(*) OVER () AS recipe_total_count
            FROM census
            WHERE lower(census.state) LIKE lower('T%')
            OR lower(census.sex) LIKE lower('T%')
            GROUP BY age,
                    sex,
                    state
            ORDER BY state,
                    sex,
                    age
            LIMIT 10
            OFFSET 40
        """,
        )


class CompareRecipeTestCase(RecipeTestCase):
    extension_classes = [CompareRecipe]

    def setUp(self):
        super().setUp()
        self.shelf = self.census_shelf

    def test_compare(self):
        """A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")
        r = r.compare(
            self.recipe()
            .metrics("pop2000")
            .dimensions("sex")
            .filters(self.census_table.c.state == "Vermont")
        )

        self.assertRecipeSQL(
            r,
            """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       avg(anon_1.pop2000) AS pop2000_compare
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex""",
        )
        self.assertRecipeCSV(
            r,
            """
        sex,pop2000,pop2000_compare,sex_id
        F,3234901,310948.0,F
        M,3059809,298532.0,M
        """,
        )

    def test_compare_custom_aggregation(self):
        """A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """
        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")
        r = r.compare(
            self.recipe()
            .metrics("pop2000_sum")
            .dimensions("sex")
            .filters(self.census_table.c.state == "Vermont")
        )

        self.assertRecipeSQL(
            r,
            """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       sum(anon_1.pop2000_sum) AS pop2000_sum_compare
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000_sum
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex""",
        )
        self.assertRecipeCSV(
            r,
            """
            sex,pop2000,pop2000_sum_compare,sex_id
            F,3234901,53483056,F
            M,3059809,51347504,M
            """,
        )

    def test_compare_suffix(self):
        """Test that the proper suffix gets added to the comparison metrics"""

        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")
        r = r.compare(
            self.recipe()
            .metrics("pop2000")
            .dimensions("sex")
            .filters(self.census_table.c.state == "Vermont"),
            suffix="_x",
        )

        self.assertRecipeSQL(
            r,
            """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       avg(anon_1.pop2000) AS pop2000_x
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2000) AS pop2000
   FROM census
   WHERE census.state = 'Vermont'
   GROUP BY sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex""",
        )
        self.assertRecipeCSV(
            r,
            """
            sex,pop2000,pop2000_x,sex_id
            F,3234901,310948.0,F
            M,3059809,298532.0,M
            """,
        )

    def test_mismatched_dimensions_raises(self):
        """Dimensions in the comparison recipe must be a subset of the
        dimensions in the base recipe"""
        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")
        r = r.compare(
            self.recipe()
            .metrics("pop2000")
            .dimensions("state")
            .filters(self.census_table.c.state == "Vermont"),
            suffix="_x",
        )

        with self.assertRaises(BadRecipe):
            r.all()


class BlendRecipeTestCase(RecipeTestCase):
    extension_classes = [BlendRecipe]

    def setUp(self):
        super().setUp()
        self.shelf = self.census_shelf

    def test_self_blend(self):
        """A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics("pop2000").dimensions("sex").order_by("sex")

        blend_recipe = (
            self.recipe()
            .metrics("pop2008")
            .dimensions("sex")
            .filters(self.census_table.c.sex == "F")
        )
        r = r.full_blend(blend_recipe, join_base="sex", join_blend="sex")

        self.assertRecipeSQL(
            r,
            """SELECT census.sex AS sex,
       sum(census.pop2000) AS pop2000,
       anon_1.pop2008 AS pop2008
FROM census
LEFT OUTER JOIN
  (SELECT census.sex AS sex,
          sum(census.pop2008) AS pop2008
   FROM census
   WHERE census.sex = 'F'
   GROUP BY sex) AS anon_1 ON census.sex = anon_1.sex
GROUP BY census.sex
ORDER BY census.sex""",
        )
        self.assertRecipeCSV(
            r,
            """
            sex,pop2000,pop2008,sex_id
            F,3234901,3499762,F
            M,3059809,,M
            """,
        )

    def test_blend(self):
        """A basic comparison recipe. The base recipe looks at all data, the
        comparison only applies to vermont

        Note: Ordering is only preserved on postgres engines.
        """

        r = self.recipe().metrics("pop2000").dimensions("state").order_by("state")

        blend_recipe = (
            self.recipe()
            .shelf(self.statefact_shelf)
            .dimensions("state", "abbreviation")
        )
        r = r.blend(blend_recipe, join_base="state", join_blend="state")

        self.assertRecipeSQL(
            r,
            """SELECT census.state AS state,
       sum(census.pop2000) AS pop2000,
       anon_1.abbreviation AS abbreviation
FROM census
JOIN
  (SELECT state_fact.abbreviation AS abbreviation,
          state_fact.name AS state
   FROM state_fact
   GROUP BY abbreviation,
            state) AS anon_1 ON census.state = anon_1.state
GROUP BY census.state,
         anon_1.abbreviation
ORDER BY census.state""",
        )
        self.assertRecipeCSV(
            r,
            """
            state,pop2000,abbreviation,abbreviation_id,state_id
            Tennessee,5685230,TN,TN,Tennessee
            Vermont,609480,VT,VT,Vermont
            """,
        )
