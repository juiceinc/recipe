from copy import copy

from sureberus.errors import SureError

from recipe import Recipe
from recipe.extensions import PaginateCountOver, PaginateInline
from tests.test_base import RecipeTestCase
from sureberus.errors import SureError


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
