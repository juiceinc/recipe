from recipe.extensions import RecipeExtension
from tests.test_base import RecipeTestCase


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
