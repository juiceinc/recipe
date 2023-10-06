from copy import copy

from sqlalchemy import func
from sureberus.errors import SureError

from recipe.exceptions import BadRecipe
from recipe.extensions import AutomaticFilters, is_compound_filter
from recipe.ingredients import Dimension, Metric
from recipe.shelf import Shelf
from tests.test_base import RecipeTestCase

from .base import convert_to_json_encoded


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

    def test_namedfilters(self):
        """Automatic filters can be passed multiple times and all will apply"""
        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": [{"namedfilter": ["babies"]}],
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            # The "babies" condition is applied
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.age < 2
GROUP BY first""",
            )

        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": [{"namedfilter": ["babies", "freds"]}],
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            # The "babies" condition AND the "freds" condition is applied
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.age < 2
  AND foo.last = 'fred'
GROUP BY first""",
            )

        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": [{"namedfilter__or": ["babies", "freds"]}],
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            # The "babies" OR "freds" condition is applied
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE foo.age < 2
  OR foo.last = 'fred'
GROUP BY first""",
            )

        for recipe in self.recipe_list(
            {
                "metrics": ["age"],
                "dimensions": ["first"],
                "automatic_filters": [
                    {"namedfilter__not": ["babies", "freds"], "last__gt": "x"}
                ],
            }
        ):
            self.assertTrue(recipe.recipe_extensions[0].apply)
            # The "babies" OR "freds" condition is applied
            self.assertRecipeSQL(
                recipe,
                """SELECT foo.first AS first,
       sum(foo.age) AS age
FROM foo
WHERE NOT (foo.age < 2
           AND foo.last = 'fred')
  AND foo.last > 'x'
GROUP BY first""",
            )

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
