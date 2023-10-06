from .base import DummyExtension
from tests.test_base import RecipeTestCase
from sureberus.errors import BadType
from recipe.exceptions import BadRecipe


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
