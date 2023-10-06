from copy import copy

from faker import Faker
from sqlalchemy import func
from sureberus.errors import SureError

from recipe import Dimension, Metric, Shelf
from recipe.extensions import Anonymize
from recipe.utils import generate_faker_seed
from tests.test_base import RecipeTestCase


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
