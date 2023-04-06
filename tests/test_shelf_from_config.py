"""
Test recipes built from yaml files in the ingredients directory.

"""

import os
import time
import warnings
from copy import deepcopy
from datetime import date

from dateutil.relativedelta import relativedelta
import yaml

from recipe import AutomaticFilters, BadIngredient, InvalidIngredient, Shelf
from tests.test_base import RecipeTestCase


class ConfigTestBase(RecipeTestCase):
    """A base class for testing shelves built from config."""

    # The directory to look for yaml config files
    yaml_location = "shelf_config"
    shelf_cache = {}

    def shelf_from_filename(self, shelf_name, selectable=None):
        """Load a file from the sample ingredients.yaml files."""
        d = os.path.dirname(os.path.realpath(__file__))
        fn = os.path.join(d, self.yaml_location, shelf_name)
        contents = open(fn).read()
        return self.shelf_from_yaml(contents, selectable)

    def shelf_from_yaml(self, yaml_config, selectable, **kwargs):
        """Create a shelf directly from configuration"""
        return Shelf.from_validated_yaml(yaml_config, selectable, **kwargs)


class TestNullHandling(ConfigTestBase):
    def test_dimension_null_handling(self):
        """Test different ways of handling nulls in dimensions"""
        shelf = self.shelf_from_filename(
            "scores_with_nulls.yaml", self.scores_with_nulls_table
        )

        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("department")
            .metrics("score")
            .order_by("department")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT scores_with_nulls.department AS department,
                avg(scores_with_nulls.score) AS score
            FROM scores_with_nulls
            GROUP BY department
            ORDER BY department""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            department,score,department_id
            ,80.0,
            ops,90.0,ops
            sales,,sales
            """,
        )

    def test_dimension_null_handling_with_lookup_default(self):
        """Test different ways of handling nulls in dimensions"""

        # This uses the lookup_default to replace items that aren't found in lookup
        # department_lookup:
        #     kind: Dimension
        #     field: department
        #     lookup:
        #         sales: Sales
        #         ops: Operations
        #     lookup_default: c

        shelf = self.shelf_from_filename(
            "scores_with_nulls.yaml", self.scores_with_nulls_table
        )
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("department_lookup")
            .metrics("score")
            .order_by("department_lookup")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT scores_with_nulls.department AS department_lookup_raw,
                avg(scores_with_nulls.score) AS score
            FROM scores_with_nulls
            GROUP BY department_lookup_raw
            ORDER BY department_lookup_raw""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            department_lookup_raw,score,department_lookup,department_lookup_id
            ,80.0,Unknown,
            ops,90.0,Operations,ops
            sales,,Sales,sales
            """,
        )

    def test_dimension_null_handling_with_null_in_lookup(self):
        """Test different ways of handling nulls in dimensions"""

        # This uses a null in the lookup
        # department_lookup_with_null:
        #     kind: Dimension
        #     field: department
        #     lookup:
        #         sales: Sales
        #         ops: Operations
        #         null: 'can not find department'

        shelf = self.shelf_from_filename(
            "scores_with_nulls.yaml", self.scores_with_nulls_table
        )
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("department_lookup_with_null")
            .metrics("score")
            .order_by("department_lookup_with_null")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT scores_with_nulls.department AS department_lookup_with_null_raw,
                avg(scores_with_nulls.score) AS score
            FROM scores_with_nulls
            GROUP BY department_lookup_with_null_raw
            ORDER BY department_lookup_with_null_raw""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            department_lookup_with_null_raw,score,department_lookup_with_null,department_lookup_with_null_id
            ,80.0,can not find department,
            ops,90.0,Operations,ops
            sales,,Sales,sales
            """,
        )

    def test_dimension_null_handling_with_default(self):
        """Test different ways of handling nulls in dimensions"""

        # This uses default which will coalesce missing values to a defined value
        # department_default:
        #     kind: Dimension
        #     field:
        #         value: department
        #         default: 'N/A'
        shelf = self.shelf_from_filename(
            "scores_with_nulls.yaml", self.scores_with_nulls_table
        )
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("department_default")
            .metrics("score")
            .order_by("department_default")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT coalesce(scores_with_nulls.department, 'N/A') AS department_default,
                avg(scores_with_nulls.score) AS score
            FROM scores_with_nulls
            GROUP BY department_default
            ORDER BY department_default""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            department_default,score,department_default_id
            N/A,80.0,N/A
            ops,90.0,ops
            sales,,sales
            """,
        )

    def test_dimension_null_handling_with_buckets(self):
        """Test different ways of handling nulls in dimensions"""

        # This uses default which will coalesce missing values to a defined value
        # department_default:
        #     kind: Dimension
        #     field:
        #         value: department
        #         default: 'N/A'
        shelf = self.shelf_from_filename(
            "scores_with_nulls.yaml", self.scores_with_nulls_table
        )
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("department_buckets")
            .metrics("score")
            .order_by("department_buckets")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
                    WHEN (scores_with_nulls.department = 'sales') THEN 'Sales'
                    WHEN (scores_with_nulls.department = 'ops') THEN 'Operations'
                    ELSE 'Other'
                END AS department_buckets,
                CASE
                    WHEN (scores_with_nulls.department = 'sales') THEN 0
                    WHEN (scores_with_nulls.department = 'ops') THEN 1
                    ELSE 9999
                END AS department_buckets_order_by,
                avg(scores_with_nulls.score) AS score
            FROM scores_with_nulls
            GROUP BY department_buckets,
                    department_buckets_order_by
            ORDER BY department_buckets_order_by,
                    department_buckets""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            department_buckets,department_buckets_order_by,score,department_buckets_id
            Sales,0,,Sales
            Operations,1,90.0,Operations
            Other,9999,80.0,Other
            """,
        )

    def test_dimension_null_handling_multi_approaches(self):
        """Test different ways of handling nulls in dimensions"""

        # This uses all null handling together (coalesce with default wins and
        # is then turned into "Unknown")
        # department_lookup_with_everything:
        #     kind: Dimension
        #     field:
        #         value: department
        #         default: 'N/A'
        #     lookup:
        #         sales: Sales
        #         ops: Operations
        #         null: 'can not find department'
        #     lookup_default: Unknown
        shelf = self.shelf_from_filename(
            "scores_with_nulls.yaml", self.scores_with_nulls_table
        )
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("department_lookup_with_everything")
            .metrics("score")
            .order_by("-department_lookup_with_everything")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT coalesce(scores_with_nulls.department, 'N/A') AS department_lookup_with_everything_raw,
                avg(scores_with_nulls.score) AS score
            FROM scores_with_nulls
            GROUP BY department_lookup_with_everything_raw
            ORDER BY department_lookup_with_everything_raw DESC""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            department_lookup_with_everything_raw,score,department_lookup_with_everything,department_lookup_with_everything_id
            sales,,Sales,sales
            ops,90.0,Operations,ops
            N/A,80.0,Unknown,N/A
            """,
        )

    def test_metric_null_handling(self):
        """Test handling nulls in metrics"""
        shelf = self.shelf_from_filename(
            "scores_with_nulls.yaml", self.scores_with_nulls_table
        )

        # score_with_default:
        #     kind: Metric
        #     field:
        #         aggregation: avg
        #         value: score
        #         default: -1.0
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("department")
            .metrics("score_with_default")
            .order_by("department")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT scores_with_nulls.department AS department,
                avg(coalesce(scores_with_nulls.score, -1.0)) AS score_with_default
            FROM scores_with_nulls
            GROUP BY department
            ORDER BY department""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            department,score_with_default,department_id
            ,39.5,
            ops,59.666666666666664,ops
            sales,-1.0,sales
            """,
        )


class TestRecipeIngredientsYamlParsed(ConfigTestBase):
    def test_ingredients1_from_validated_yaml(self):
        shelf = self.shelf_from_filename("ingredients1.yaml", self.basic_table)
        recipe = self.recipe(shelf=shelf).metrics("age").dimensions("first")
        self.assertRecipeCSV(
            recipe,
            """
            first,age,first_id
            hi,15,hi
            """,
        )

    def test_ingredients1_from_yaml(self):
        shelf = self.shelf_from_filename("ingredients1.yaml", self.basic_table)
        recipe = self.recipe(shelf=shelf).metrics("age").dimensions("first")
        self.assertRecipeCSV(
            recipe,
            """
            first,age,first_id
            hi,15,hi
            """,
        )

    def test_ingredients1_between_dates(self):
        shelf = self.shelf_from_filename("ingredients1.yaml", self.basic_table)
        recipe = self.recipe(shelf=shelf).metrics("date_between")
        today = date.today()
        self.assertRecipeSQL(
            recipe,
            f"""SELECT sum(CASE
               WHEN (foo.birth_date BETWEEN '{date(today.year - 20, today.month, today.day)}' AND '{today}') THEN foo.age
           END) AS date_between
            FROM foo""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            date_between
            15
            """,
        )

        # dt_between is a datetime, it generates the same query with
        # exact datetimes, we don't try to check the exact sql but it will
        # look like
        #         SELECT sum(CASE
        #                WHEN (foo.dt BETWEEN '1999-06-23 12:13:01.819190'
        #                   AND '2019-06-23 12:13:01.820635') THEN foo.age
        #            END) AS dt_between
        # FROM foo
        recipe = self.recipe(shelf=shelf).metrics("dt_between")
        assert (
            recipe.to_sql()
            != """SELECT sum(CASE
               WHEN (foo.birth_date BETWEEN '{}' AND '{}') THEN foo.age
           END) AS date_between
FROM foo""".format(
                date(today.year - 20, today.month, today.day), today
            )
        )
        assert str(date(today.year - 20, today.month, today.day)) in recipe.to_sql()
        assert str(today) in recipe.to_sql()
        self.assertRecipeCSV(
            recipe,
            """dt_between
15
""",
        )

    def test_census_from_validated_yaml(self):
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        self.assertRecipeCSV(
            recipe,
            """
            state,pop2000,ttlpop,state_id
            Tennessee,5685230,11887637,Tennessee
            Vermont,609480,1230082,Vermont
            """,
        )

    def test_census_from_yaml(self):
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        self.assertRecipeCSV(
            recipe,
            """
            state,pop2000,ttlpop,state_id
            Tennessee,5685230,11887637,Tennessee
            Vermont,609480,1230082,Vermont
            """,
        )

    def test_nested_census_from_validated_yaml(self):
        """Build a recipe that depends on the results of another recipe"""
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        nested_shelf = self.shelf_from_filename("census_nested.yaml", recipe)
        nested_recipe = self.recipe(shelf=nested_shelf).metrics("ttlpop", "num_states")
        self.assertRecipeSQL(
            nested_recipe,
            """SELECT count(anon_1.state) AS num_states,
                sum(anon_1.ttlpop) AS ttlpop
            FROM
            (SELECT census.state AS state,
                    sum(census.pop2000) AS pop2000,
                    sum(census.pop2000 + census.pop2008) AS ttlpop
            FROM census
            GROUP BY state
            ORDER BY state) AS anon_1
            """,
        )
        self.assertRecipeCSV(
            nested_recipe,
            """
            num_states,ttlpop
            2,13117719
            """,
        )

    def test_nested_census_from_yaml(self):
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        nested_shelf = self.shelf_from_filename("census_nested.yaml", recipe)
        nested_recipe = self.recipe(shelf=nested_shelf).metrics("ttlpop", "num_states")
        self.assertRecipeCSV(
            nested_recipe,
            """
            num_states,ttlpop
            2,13117719
            """,
        )

    def test_census_condition_between(self):
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = self.recipe(shelf=shelf).metrics("teenagers")
        self.assertRecipeSQL(
            recipe,
            """SELECT sum(CASE
                            WHEN (census.age BETWEEN 13 AND 19) THEN census.pop2000
                        END) AS teenagers
                FROM census""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            teenagers
            614548
            """,
        )

    def test_census_condition_between_dates(self):
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = self.recipe(shelf=shelf).metrics("teenagers")
        self.assertRecipeSQL(
            recipe,
            """SELECT sum(CASE
                            WHEN (census.age BETWEEN 13 AND 19) THEN census.pop2000
                        END) AS teenagers
                FROM census""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            teenagers
            614548
            """,
        )

    def test_census_mixed_buckets(self):
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("mixed_buckets")
            .metrics("pop2000")
            .order_by("-mixed_buckets")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
           WHEN (census.state IN ('Vermont',
                                  'New Hampshire')) THEN 'northeast'
           WHEN (census.age < 2) THEN 'babies'
           WHEN (census.age IN (2,
                                3,
                                4,
                                5,
                                6,
                                7,
                                8,
                                9,
                                10,
                                11,
                                12)) THEN 'children'
           WHEN (census.age < 20) THEN 'teens'
           ELSE 'oldsters'
       END AS mixed_buckets,
       CASE
           WHEN (census.state IN ('Vermont',
                                  'New Hampshire')) THEN 0
           WHEN (census.age < 2) THEN 1
           WHEN (census.age IN (2,
                                3,
                                4,
                                5,
                                6,
                                7,
                                8,
                                9,
                                10,
                                11,
                                12)) THEN 2
           WHEN (census.age < 20) THEN 3
           ELSE 9999
       END AS mixed_buckets_order_by,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY mixed_buckets,
         mixed_buckets_order_by
ORDER BY mixed_buckets_order_by DESC,
         mixed_buckets DESC""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            mixed_buckets,mixed_buckets_order_by,pop2000,mixed_buckets_id
            oldsters,9999,4124620,oldsters
            teens,3,550515,teens
            children,2,859206,children
            babies,1,150889,babies
            northeast,0,609480,northeast
            """,
        )

    def test_census_buckets_ordering(self):
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("age_buckets")
            .metrics("pop2000")
            .order_by("age_buckets")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
           WHEN (census.age < 2) THEN 'babies'
           WHEN (census.age < 13) THEN 'children'
           WHEN (census.age < 20) THEN 'teens'
           ELSE 'oldsters'
       END AS age_buckets,
       CASE
           WHEN (census.age < 2) THEN 0
           WHEN (census.age < 13) THEN 1
           WHEN (census.age < 20) THEN 2
           ELSE 9999
       END AS age_buckets_order_by,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY age_buckets,
         age_buckets_order_by
ORDER BY age_buckets_order_by,
         age_buckets""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            age_buckets,age_buckets_order_by,pop2000,age_buckets_id
            babies,0,164043,babies
            children,1,948240,children
            teens,2,614548,teens
            oldsters,9999,4567879,oldsters
            """,
        )
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("age_buckets")
            .metrics("pop2000")
            .order_by("-age_buckets")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
           WHEN (census.age < 2) THEN 'babies'
           WHEN (census.age < 13) THEN 'children'
           WHEN (census.age < 20) THEN 'teens'
           ELSE 'oldsters'
       END AS age_buckets,
       CASE
           WHEN (census.age < 2) THEN 0
           WHEN (census.age < 13) THEN 1
           WHEN (census.age < 20) THEN 2
           ELSE 9999
       END AS age_buckets_order_by,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY age_buckets,
         age_buckets_order_by
ORDER BY age_buckets_order_by DESC,
         age_buckets DESC""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            age_buckets,age_buckets_order_by,pop2000,age_buckets_id
            oldsters,9999,4567879,oldsters
            teens,2,614548,teens
            children,1,948240,children
            babies,0,164043,babies
            """,
        )

    def test_census_buckets_nolabel(self):
        """If not default label is provided, buckets default to "Not found" """
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("age_buckets_nolabel")
            .metrics("pop2000")
            .order_by("age_buckets_nolabel")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
           WHEN (census.age < 2) THEN 'babies'
           WHEN (census.age < 13) THEN 'children'
           WHEN (census.age < 20) THEN 'teens'
           ELSE 'Not found'
       END AS age_buckets_nolabel,
       CASE
           WHEN (census.age < 2) THEN 0
           WHEN (census.age < 13) THEN 1
           WHEN (census.age < 20) THEN 2
           ELSE 9999
       END AS age_buckets_nolabel_order_by,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY age_buckets_nolabel,
         age_buckets_nolabel_order_by
ORDER BY age_buckets_nolabel_order_by,
         age_buckets_nolabel""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            age_buckets_nolabel,age_buckets_nolabel_order_by,pop2000,age_buckets_nolabel_id
            babies,0,164043,babies
            children,1,948240,children
            teens,2,614548,teens
            Not found,9999,4567879,Not found
            """,
        )

    def test_complex_census_from_validated_yaml(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics"""
        shelf = self.shelf_from_filename("census_complex.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("pop2000")
            .order_by("state")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT census.state AS state_raw,
       sum(CASE
               WHEN (census.age > 40) THEN census.pop2000
           END) AS pop2000
FROM census
GROUP BY state_raw
ORDER BY state_raw""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            state_raw,pop2000,state,state_id
            Tennessee,2392122,The Volunteer State,Tennessee
            Vermont,271469,The Green Mountain State,Vermont
            """,
        )

    def test_complex_census_quickselect_from_validated_yaml(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics and quickselects"""
        shelf = self.shelf_from_filename("census_complex.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf, extension_classes=(AutomaticFilters,))
            .dimensions("state")
            .metrics("pop2008")
            .order_by("state")
            .automatic_filters({"state__quickselect": "younger"})
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT census.state AS state_raw,
                sum(census.pop2008) AS pop2008
            FROM census
            WHERE census.age < 40
            GROUP BY state_raw
            ORDER BY state_raw""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            state_raw,pop2008,state,state_id
            Tennessee,3297299,The Volunteer State,Tennessee
            Vermont,300605,The Green Mountain State,Vermont
            """,
        )
        recipe = (
            self.recipe(shelf=shelf, extension_classes=(AutomaticFilters,))
            .dimensions("state")
            .metrics("pop2008")
            .order_by("state")
            .automatic_filters({"state__quickselect": "vermontier"})
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT census.state AS state_raw,
                sum(census.pop2008) AS pop2008
            FROM census
            WHERE census.state = 'Vermont'
            GROUP BY state_raw
            ORDER BY state_raw""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            state_raw,pop2008,state,state_id
            Vermont,620602,The Green Mountain State,Vermont
            """,
        )

    def test_shelf_with_condition_references(self):
        """Build a recipe using a shelf that uses condition references"""
        shelf = self.shelf_from_filename("census_references.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("pop2008oldsters")
            .order_by("state")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT census.state AS state_raw,
                sum(CASE
                        WHEN (census.age > 40) THEN census.pop2008
                    END) AS pop2008oldsters
            FROM census
            GROUP BY state_raw
            ORDER BY state_raw""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            state_raw,pop2008oldsters,state,state_id
            Tennessee,2821955,The Volunteer State,Tennessee
            Vermont,311842,The Green Mountain State,Vermont
            """,
        )

    def test_deprecated_ingredients_idvaluedim2(self):
        """Test deprecated ingredient kinds in a yaml file"""
        shelf = self.shelf_from_filename("census_deprecated.yaml", self.census_table)

        # We can IdValueDimension
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("avgage")
            .order_by("state_idval")
        )
        # If the order_by isn't used in the recipe, it is ignored
        self.assertRecipeSQLNotContains(recipe, "state_idval")

    def test_bad_census(self):
        """Test a bad yaml file"""
        shelf = self.shelf_from_filename("census_bad.yaml", self.census_table)
        self.assertIsInstance(shelf["pop2000"], InvalidIngredient)
        assert "No terminal" in shelf["pop2000"].error["extra"]["details"]

    def test_bad_census_in(self):
        """Test a bad yaml file"""
        shelf = self.shelf_from_filename("census_bad_in.yaml", self.census_table)
        self.assertIsInstance(shelf["pop2000"], InvalidIngredient)
        assert "No terminal" in shelf["pop2000"].error["extra"]["details"]

    def test_shelf_with_invalidcolumn(self):
        """Build a recipe using a shelf that uses field references"""
        shelf = self.shelf_from_filename("census_references.yaml", self.census_table)
        self.assertIsInstance(shelf["badfield"], InvalidIngredient)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("badfield")
            .order_by("state")
        )
        with self.assertRaises(BadIngredient):
            recipe.to_sql()

    def test_census_buckets(self):
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        recipe = self.recipe(shelf=shelf).dimensions("age_buckets").metrics("pop2000")
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
                    WHEN (census.age < 2) THEN 'babies'
                    WHEN (census.age < 13) THEN 'children'
                    WHEN (census.age < 20) THEN 'teens'
                    ELSE 'oldsters'
                END AS age_buckets,
                CASE
                    WHEN (census.age < 2) THEN 0
                    WHEN (census.age < 13) THEN 1
                    WHEN (census.age < 20) THEN 2
                    ELSE 9999
                END AS age_buckets_order_by,
                sum(census.pop2000) AS pop2000
            FROM census
            GROUP BY age_buckets,
                    age_buckets_order_by
            ORDER BY age_buckets_order_by,
                    age_buckets""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            age_buckets,age_buckets_order_by,pop2000,age_buckets_id
            babies,0,164043,babies
            children,1,948240,children
            teens,2,614548,teens
            oldsters,9999,4567879,oldsters
            """,
        )

    def test_deprecated_ingredients_dividemetric(self):
        """Skip this Deprecated ingredient kinds are not supperted in version 2"""
        pass

    def test_deprecated_ingredients_lookupdimension(self):
        """Skip this Deprecated ingredient kinds are not supperted in version 2"""
        pass

    def test_deprecated_ingredients_idvaluedimension(self):
        """Skip this Deprecated ingredient kinds are not supperted in version 2"""
        pass

    def test_deprecated_ingredients_wtdavgmetric(self):
        """Skip this Deprecated ingredient kinds are not supperted in version 2"""
        pass

    def test_shelf_with_references(self):
        """Parsed shelves do division slighly differently"""
        shelf = self.shelf_from_filename("census_references.yaml", self.census_table)
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state")
            .metrics("popdivide")
            .order_by("state")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT census.state AS state_raw,
                CASE
                    WHEN (sum(census.pop2008) = 0) THEN NULL
                    ELSE CAST(sum(CASE
                                        WHEN (census.age > 40) THEN census.pop2000
                                    END) AS FLOAT) / CAST(sum(census.pop2008) AS FLOAT)
                END AS popdivide
            FROM census
            GROUP BY state_raw
            ORDER BY state_raw""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            state_raw,popdivide,state,state_id
            Tennessee,0.38567639950103244,The Volunteer State,Tennessee
            Vermont,0.4374284968466102,The Green Mountain State,Vermont
            """,
        )

    def test_shelf_with_invalidingredient(self):
        """Build a recipe using a shelf that uses field references"""
        shelf = self.shelf_from_filename("census.yaml", self.census_table)
        self.assertIsInstance(shelf["baddim"], InvalidIngredient)
        recipe = self.recipe(shelf=shelf).dimensions("baddim").metrics("pop2000")
        # Trying to run the recipe raises an exception with the bad ingredient details
        with self.assertRaises(BadIngredient):
            recipe.to_sql()

    def test_complex_census_from_validated_yaml_math(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics"""
        shelf = self.shelf_from_filename("census_complex.yaml", self.census_table)
        recipe = self.recipe(shelf=shelf).dimensions("state").metrics("allthemath")
        self.assertRecipeSQL(
            recipe,
            """SELECT census.state AS state_raw,
                sum(census.pop2000 + (census.pop2008 - census.pop2000 * CASE
                    WHEN (census.pop2000 = 0) THEN NULL
                    ELSE CAST(census.pop2008 AS FLOAT) / CAST(census.pop2000 AS FLOAT)
                END)) AS allthemath
            FROM census
            GROUP BY state_raw""",
        )  # noqa: E501
        self.assertRecipeCSV(
            recipe,
            """
            state_raw,allthemath,state,state_id
            Tennessee,5685230.0,The Volunteer State,Tennessee
            Vermont,609480.0,The Green Mountain State,Vermont
            """,
        )

    def test_deprecated_ingredients_idvaluedim(self):
        """Test deprecated ingredient kinds in a yaml file"""
        shelf = self.shelf_from_filename("census_deprecated.yaml", self.census_table)

        # We can IdValueDimension
        recipe = (
            self.recipe(shelf=shelf)
            .dimensions("state_idval")
            .metrics("avgage")
            .order_by("state_idval")
            .limit(10)
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT census.pop2000 AS state_idval_id,
                census.state AS state_idval,
                CASE
                    WHEN (sum(census.pop2000) = 0) THEN NULL
                    ELSE CAST(sum(census.age * census.pop2000) AS FLOAT) / CAST(sum(census.pop2000) AS FLOAT)
                END AS avgage
            FROM census
            GROUP BY state_idval_id,
                    state_idval
            ORDER BY state_idval,
                    state_idval_id
            LIMIT 10
            OFFSET 0""",
        )

        # Parsed shelves provide better division
        self.assertRecipeCSV(
            recipe,
            """
            state_idval_id,state_idval,avgage
            5033,Tennessee,84.0
            5562,Tennessee,83.0
            6452,Tennessee,82.0
            7322,Tennessee,81.0
            8598,Tennessee,80.0
            9583,Tennessee,79.0
            10501,Tennessee,84.0
            10672,Tennessee,78.0
            11141,Tennessee,83.0
            11168,Tennessee,77.0
            """,
        )

    def test_is(self):
        """Test fields using is"""
        shelf = self.shelf_from_filename("ingredients1.yaml", self.basic_table)
        recipe = self.recipe(shelf=shelf).metrics("dt_test").dimensions("first")
        self.assertRecipeSQL(
            recipe,
            """SELECT foo.first AS first,
                sum(CASE
                        WHEN (foo.birth_date IS NULL) THEN foo.age
                        ELSE 1
                    END) AS dt_test
            FROM foo
            GROUP BY first""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            first,dt_test,first_id
            hi,2,hi
            """,
        )

    def test_intelligent_date(self):
        """Test intelligent dates like `date is last year`"""
        shelf = self.shelf_from_filename("ingredients1.yaml", self.basic_table)
        recipe = (
            self.recipe(shelf=shelf)
            .metrics("intelligent_date_test")
            .dimensions("first")
        )
        from datetime import date

        from dateutil.relativedelta import relativedelta

        today = date.today()
        start_dt = date(today.year - 1, 1, 1)
        end_dt = start_dt + relativedelta(years=1, days=-1)
        self.assertRecipeSQL(
            recipe,
            f"""SELECT foo.first AS first,
                sum(CASE
                        WHEN (foo.birth_date BETWEEN '{start_dt}' AND '{end_dt}') THEN foo.age
                        ELSE 2
                    END) AS intelligent_date_test
            FROM foo
            GROUP BY first""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            first,intelligent_date_test,first_id
            hi,4,hi
            """,
        )


class TestMultipleSelectables(ConfigTestBase):
    def test_multi_selectable(self):
        """A shelf can reference multiple selectables"""
        shelf_config = """
username:
  kind: Dimension
  field: username
department:
  kind: Dimension
  field: department
testid:
  kind: Dimension
  field: testid
score:
  kind: Metric
  field: avg(score)
tagscorestest_cnt:
  kind: Metric
  field: count_distinct(tagscores.testid)
tagscoresdepartment:
  kind: Dimension
  field: tagscores.department
  filter: tagscores.username = username
tagscorestestid:
  kind: Dimension
  field: tagscores.testid
  filter: tagscores.username = username
tagscoresscore:
  kind: Metric
  field: min(tagscores.score)
  filter: tagscores.username = username
        """

        shelf = self.shelf_from_yaml(
            shelf_config,
            self.scores_with_nulls_table,
            extra_selectables=[(self.tagscores_table, "tagscores")],
        )

        recipe = self.recipe(shelf=shelf).metrics("score").dimensions("department")
        self.assertRecipeSQL(
            recipe,
            """
            SELECT scores_with_nulls.department AS department,
                avg(scores_with_nulls.score) AS score
            FROM scores_with_nulls
            GROUP BY department
        """,
        )
        self.assertRecipeCSV(
            recipe,
            """
            department,score,department_id
            ,80.0,
            ops,90.0,ops
            sales,,sales
            """,
        )

        # Now use multiple tables
        # We are doing a cross join BUT the filter key on
        # tagscorestestid allows us to join the two tables.
        # Note: Shelves created from config will implicitly get a _select_from
        # and will not raise an BadRecipe when selecting from multiple tables.
        recipe = self.recipe(shelf=shelf).metrics("score").dimensions("tagscorestestid")
        self.assertRecipeSQL(
            recipe,
            """
SELECT tagscores.testid AS tagscorestestid,
       avg(scores_with_nulls.score) AS score
FROM tagscores,
     scores_with_nulls
WHERE tagscores.username = scores_with_nulls.username
GROUP BY tagscorestestid
        """,
        )

        # Now use multiple tables
        # We are doing a cross join BUT the filter key on
        # tagscorestestid allows us to join the two tables.
        # Note: Shelves created from config will implicitly get a _select_from
        # and will not raise an BadRecipe when selecting from multiple tables.
        recipe = self.recipe(shelf=shelf).metrics("score").dimensions("tagscorestestid")
        self.assertRecipeSQL(
            recipe,
            """
SELECT tagscores.testid AS tagscorestestid,
       avg(scores_with_nulls.score) AS score
FROM tagscores,
     scores_with_nulls
WHERE tagscores.username = scores_with_nulls.username
GROUP BY tagscorestestid
        """,
        )

        # We can select multiple dimensions and metrics from both tables
        # We will not duplicate the join filter as long as it has the same sql representation
        recipe = (
            self.recipe(shelf=shelf)
            .metrics("score", "tagscoresscore")
            .dimensions("tagscorestestid", "tagscoresdepartment")
        )
        self.assertRecipeSQL(
            recipe,
            """
SELECT tagscores.department AS tagscoresdepartment,
       tagscores.testid AS tagscorestestid,
       avg(scores_with_nulls.score) AS score,
       min(tagscores.score) AS tagscoresscore
FROM tagscores,
     scores_with_nulls
WHERE tagscores.username = scores_with_nulls.username
GROUP BY tagscoresdepartment,
         tagscorestestid
        """,
        )
        self.assertRecipeCSV(
            recipe,
            """
tagscoresdepartment,tagscorestestid,score,tagscoresscore,tagscoresdepartment_id,tagscorestestid_id
ops,2,90.0,80.0,ops,2
ops,3,90.0,90.0,ops,3
ops,4,90.0,100.0,ops,4
ops,5,80.0,80.0,ops,5
ops,6,80.0,90.0,ops,6
sales,1,,80.0,sales,1
            """,
        )

        # FIXME: If we try to only use the tagscores table, we get a crossjoin.
        # It would be better if we only added the filter conditionally.
        recipe = (
            self.recipe(shelf=shelf)
            .metrics("tagscoresscore")
            .dimensions("tagscoresdepartment")
        )
        self.assertRecipeSQL(
            recipe,
            """
SELECT tagscores.department AS tagscoresdepartment,
       min(tagscores.score) AS tagscoresscore
FROM tagscores,
     scores_with_nulls
WHERE tagscores.username = scores_with_nulls.username
GROUP BY tagscoresdepartment
        """,
        )
        self.assertRecipeCSV(
            recipe,
            """
tagscoresdepartment,tagscoresscore,tagscoresdepartment_id
ops,80.0,ops
sales,80.0,sales
            """,
        )


class TestParsedSQLGeneration(ConfigTestBase):
    """More tests of SQL generation on complex parsed expressions"""

    def test_weird_table_with_column_named_true(self):
        shelf = self.shelf_from_yaml(
            """
"true":
    kind: Dimension
    field: "[true]"
            """,
            self.weird_table_with_column_named_true_table,
        )

        recipe = self.recipe(shelf=shelf).dimensions("true")
        self.assertRecipeSQL(
            recipe,
            """SELECT weird_table_with_column_named_true."true" AS "true"
            FROM weird_table_with_column_named_true
            GROUP BY "true"
            """,
        )

    def test_included_filter(self):
        """Test ingredient definitions that include a filter expression"""
        shelf = self.shelf_from_yaml(
            """
username:
    kind: Dimension
    field: username
    filter: username = "foo"
""",
            self.scores_with_nulls_table,
        )
        recipe = self.recipe(shelf=shelf).dimensions("username")
        self.assertRecipeSQL(
            recipe,
            """SELECT scores_with_nulls.username AS username
FROM scores_with_nulls
WHERE scores_with_nulls.username = 'foo'
GROUP BY username""",
        )

        # Filters that aren't datatype=bool raise errors.
        with self.assertRaises(BadIngredient):
            shelf = self.shelf_from_yaml(
                """
    username:
        kind: Dimension
        field: username
        filter: username + "moo"
    """,
                self.scores_with_nulls_table,
            )

    def test_complex_field(self):
        """Test parsed field definitions that use math, field references and more"""
        shelf = self.shelf_from_yaml(
            """
username:
    kind: Dimension
    field: username
count_star:
    kind: Metric
    field: "count(*)"
convertdate:
    kind: Dimension
    field: "month(test_date)"
strings:
    kind: Dimension
    field: "string(test_date)+string(score)"
total_nulls:
    kind: Metric
    field: "count_distinct(if(score IS NULL, username))"
chip_nulls:
    kind: Metric
    field: 'sum(if(score IS NULL and username = \"chip\",1,0))'
user_null_counter:
    kind: Metric
    field: 'if(username IS NULL, 1, 0)'
chip_or_nulls:
    kind: Metric
    field: 'sum(if(score IS NULL OR (username = \"chip\"),1,0))'
simple_math:
    kind: Metric
    field: "@count_star +  @total_nulls   + @chip_nulls"
refs_division:
    kind: Metric
    field: "@count_star / 100.0"
refs_as_denom:
    kind: Metric
    field: "12 / @count_star"
math:
    kind: Metric
    field: "(@count_star / @count_star) + (5.0 / 2.0)"
parentheses:
    kind: Metric
    field: "@count_star / (@count_star + (12.0 / 2.0))"
""",
            self.scores_with_nulls_table,
        )
        recipe = self.recipe(shelf=shelf).metrics("count_star")
        self.assertRecipeSQL(
            recipe,
            """SELECT count(*) AS count_star
            FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(
            recipe,
            """
        count_star
        6
        """,
        )

        recipe = self.recipe(shelf=shelf).metrics("total_nulls")
        self.assertRecipeSQL(
            recipe,
            """SELECT count(DISTINCT CASE
                WHEN (scores_with_nulls.score IS NULL) THEN scores_with_nulls.username
            END) AS total_nulls
            FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(
            recipe,
            """
        total_nulls
        3
        """,
        )

        recipe = self.recipe(shelf=shelf).metrics("chip_nulls")
        self.assertRecipeSQL(
            recipe,
            """SELECT sum(CASE
               WHEN (scores_with_nulls.score IS NULL
                     AND scores_with_nulls.username = 'chip') THEN 1
               ELSE 0
           END) AS chip_nulls
FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(
            recipe,
            """
        chip_nulls
        1
        """,
        )

        recipe = self.recipe(shelf=shelf).metrics("chip_or_nulls")
        self.assertRecipeSQL(
            recipe,
            """SELECT sum(CASE
                        WHEN (scores_with_nulls.score IS NULL
                                OR scores_with_nulls.username = 'chip') THEN 1
                        ELSE 0
                    END) AS chip_or_nulls
            FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            chip_or_nulls
            5
            """,
        )

        recipe = self.recipe(shelf=shelf).metrics("user_null_counter")
        self.assertRecipeSQL(
            recipe,
            """SELECT sum(CASE
                        WHEN (scores_with_nulls.username IS NULL) THEN 1
                        ELSE 0
                    END) AS user_null_counter
            FROM scores_with_nulls""",
        )

        self.assertRecipeCSV(
            recipe,
            """
            user_null_counter
            0
            """,
        )

        recipe = self.recipe(shelf=shelf).metrics("simple_math")
        self.assertRecipeSQL(
            recipe,
            """SELECT count(*) + count(DISTINCT CASE
                WHEN (scores_with_nulls.score IS NULL) THEN scores_with_nulls.username
            END) + sum(CASE
                WHEN (scores_with_nulls.score IS NULL
                    AND scores_with_nulls.username = 'chip') THEN 1
                ELSE 0
            END) AS simple_math
            FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(recipe, "simple_math\n10\n")

        recipe = self.recipe(shelf=shelf).metrics("refs_division")
        self.assertRecipeSQL(
            recipe,
            """SELECT CAST(count(*) AS FLOAT) / 100.0 AS refs_division
            FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(
            recipe,
            """
        refs_division
        0.06
        """,
        )

        recipe = self.recipe(shelf=shelf).metrics("refs_as_denom")
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
                    WHEN (count(*) = 0) THEN NULL
                    ELSE 12 / CAST(count(*) AS FLOAT)
                END AS refs_as_denom
            FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(recipe, "refs_as_denom\n2.0\n")

        recipe = self.recipe(shelf=shelf).metrics("math")
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
                    WHEN (count(*) = 0) THEN NULL
                    ELSE CAST(count(*) AS FLOAT) / CAST(count(*) AS FLOAT)
                END + 2.5 AS math
            FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(recipe, "math\n3.5\n")

        recipe = self.recipe(shelf=shelf).metrics("parentheses")
        self.assertRecipeSQL(
            recipe,
            """SELECT CASE
                    WHEN (count(*) + 6.0 = 0) THEN NULL
                    ELSE CAST(count(*) AS FLOAT) / CAST(count(*) + 6.0 AS FLOAT)
                END AS parentheses
            FROM scores_with_nulls""",
        )
        self.assertRecipeCSV(recipe, "parentheses\n0.5\n")

        recipe = (
            self.recipe(shelf=shelf).dimensions("convertdate").metrics("count_star")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT date_trunc('month', scores_with_nulls.test_date) AS convertdate,
                count(*) AS count_star
            FROM scores_with_nulls
            GROUP BY convertdate""",
        )
        # Can't run this against sqlite so we don't test csv

        recipe = self.recipe(shelf=shelf).dimensions("strings")
        self.assertRecipeSQL(
            recipe,
            """SELECT CAST(scores_with_nulls.test_date AS VARCHAR) || CAST(scores_with_nulls.score AS VARCHAR) AS strings
            FROM scores_with_nulls
            GROUP BY strings""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            strings,strings_id
            ,
            2005-01-0480.0,2005-01-0480.0
            2005-01-07100.0,2005-01-07100.0
            2005-02-0180.0,2005-02-0180.0
            """,
        )

    def test_selectables(self):
        """Test parsed field definitions built on top of other selectables"""
        shelf = self.shelf_from_yaml(
            """
username:
    kind: Dimension
    field: username
count_star:
    kind: Metric
    field: "count(*)"
""",
            self.scores_with_nulls_table,
        )
        recipe = self.recipe(shelf=shelf).dimensions("username").metrics("count_star")
        self.assertRecipeSQL(
            recipe,
            """SELECT scores_with_nulls.username AS username,
                count(*) AS count_star
            FROM scores_with_nulls
            GROUP BY username""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            username,count_star,username_id
            annika,2,annika
            chip,3,chip
            chris,1,chris
            """,
        )

        # Build a recipe using the first recipe
        shelf2 = self.shelf_from_yaml(
            """
count_star:
    kind: Metric
    field: "count(*)"
count_username:
    kind: Metric
    field: "count(username)"
""",
            recipe,
        )
        recipe2 = self.recipe(shelf=shelf2).metrics("count_star", "count_username")
        self.assertRecipeSQL(
            recipe2,
            """SELECT count(*) AS count_star,
                count(anon_1.username) AS count_username
            FROM
            (SELECT scores_with_nulls.username AS username,
                    count(*) AS count_star
            FROM scores_with_nulls
            GROUP BY username) AS anon_1""",
        )
        self.assertRecipeCSV(recipe2, "count_star,count_username\n3,3\n")


class TestShelfConstants(ConfigTestBase):
    def test_simple_constant(self):
        cache = Cache()
        shelf = self.shelf_from_yaml(
            """
            username: {kind: Dimension, field: username+constants.twostr}
            count_star: {kind: Metric, field: count(*) * constants.sumscore}
            count_star_times_two: {kind: Metric, field: constants.two*count(*)}
            convertdate: {kind: Dimension, field: month(test_date)}
            """,
            self.scores_with_nulls_table,
            ingredient_cache=cache,
            constants={
                "two": 2,
                "twofloat": 2.0,
                "twostr": "two",
                "sumscore": "sum(score)",
            },
        )
        recipe = (
            self.recipe(shelf=shelf)
            .metrics("count_star", "count_star_times_two")
            .dimensions("username")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT scores_with_nulls.username || CAST('two' AS VARCHAR) AS username,
       count(*) * constants.sumscore AS count_star,
       CAST(2 AS INTEGER) * count(*) AS count_star_times_two
FROM scores_with_nulls,

  (SELECT sum(scores_with_nulls.score) AS sumscore
   FROM scores_with_nulls) AS constants
GROUP BY username""",
        )
        self.assertRecipeCSV(
            recipe,
            """username,count_star,count_star_times_two,username_id
annikatwo,520.0,4,annikatwo
chiptwo,780.0,6,chiptwo
christwo,260.0,2,christwo""",
        )

    def test_census_constants(self):
        cache = Cache()
        shelf = self.shelf_from_yaml(
            """
            state: {kind: Dimension, field: state}
            pop2000: {kind: Metric, field: sum(pop2000)}
            pop2000_of_total: {kind: Metric, field: sum(pop2000)/constants.ttlpop}
            """,
            self.census_table,
            ingredient_cache=cache,
            constants={"ttlpop": "sum(pop2000)"},
        )
        recipe = self.recipe(shelf=shelf).metrics("pop2000").dimensions("state")
        self.assertRecipeSQL(
            recipe,
            """SELECT census.state AS state,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY state""",
        )
        self.assertRecipeCSV(
            recipe,
            """state,pop2000,state_id
Tennessee,5685230,Tennessee
Vermont,609480,Vermont""",
        )
        recipe = (
            self.recipe(shelf=shelf)
            .metrics("pop2000", "pop2000_of_total")
            .dimensions("state")
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT census.state AS state,
       sum(census.pop2000) AS pop2000,
       CASE
           WHEN (constants.ttlpop = 0) THEN NULL
           ELSE CAST(sum(census.pop2000) AS FLOAT) / CAST(constants.ttlpop AS FLOAT)
       END AS pop2000_of_total
FROM census,

  (SELECT sum(census.pop2000) AS ttlpop
   FROM census) AS constants
GROUP BY state""",
        )
        self.assertRecipeCSV(
            recipe,
            """state,pop2000,pop2000_of_total,state_id
Tennessee,5685230,0.9031758413016644,Tennessee
Vermont,609480,0.09682415869833559,Vermont""",
        )


class Cache(dict):
    def set(self, k, v):
        self[k] = v


class TestCache(ConfigTestBase):
    def test_cache(self):
        cache = Cache()
        self.shelf_from_yaml(
            """
            username: {kind: Dimension, field: username}
            count_star: {kind: Metric, field: count(*)}
            convertdate: {kind: Dimension, field: month(test_date)}
            """,
            self.scores_with_nulls_table,
            ingredient_cache=cache,
        )
        self.assertEqual(len(cache), 1)
        ingredients = cache[list(cache.keys())[0]]
        self.assertEqual(len(ingredients), 3)

    def test_selectables_cache(self):
        """Test cache when the selectable is a recipe"""
        cache = Cache()
        shelf = self.shelf_from_yaml(
            """
            username: {kind: Dimension, field: username}
            count_star: {kind: Metric, field: 'count(*)'}
            """,
            self.scores_with_nulls_table,
            ingredient_cache=cache,
        )
        recipe = self.recipe(shelf=shelf).dimensions("username").metrics("count_star")

        self.assertEqual(len(cache), 1)
        first_cache_key = list(cache.keys())[0]
        self.assertEqual(len(cache[first_cache_key]), 2)

        # Build a recipe using the first recipe
        self.shelf_from_yaml(
            "count_username: {kind: Metric, field: 'count(username)'}",
            recipe,
            ingredient_cache=cache,
        )

        self.assertEqual(len(cache), 2)
        second_cache_key = list(cache.keys() - {first_cache_key})[0]
        self.assertEqual(len(cache[second_cache_key]), 1)

    def test_cache_is_faster(self):
        yml = """
        username: {kind: Dimension, field: username}
        count_star: {kind: Metric, field: "count(*)"}
        convertdate: {kind: Dimension, field: "month(test_date)"}
        strings: {kind: Dimension, field: "string(test_date)+string(score)"}
        total_nulls: {kind: Metric, field: "count_distinct(if(score IS NULL, username))"}
        chip_nulls: {kind: Metric, field: 'sum(if(score IS NULL and username = "chip",1,0))'}
        user_null_counter: {kind: Metric, field: 'if(username IS NULL, 1, 0)'}
        chip_or_nulls: {kind: Metric, field: 'sum(if(score IS NULL OR (username = "chip"),1,0))'}
        simple_math: {kind: Metric, field: "@count_star +  @total_nulls   + @chip_nulls"}
        refs_division: {kind: Metric, field: "@count_star / 100.0"}
        refs_as_denom: {kind: Metric, field: "12 / @count_star"}
        math: {kind: Metric, field: "(@count_star / @count_star) + (5.0 / 2.0)"}
        parentheses: {kind: Metric, field: "@count_star / (@count_star + (12.0 / 2.0))"}
        """
        config = yaml.safe_load(yml)
        uncached_start = time.time()
        COUNT = 2
        for i in range(COUNT):
            Shelf.from_config(deepcopy(config), self.scores_with_nulls_table)
        uncached_duration = time.time() - uncached_start

        cache = Cache()
        # prime the cache
        Shelf.from_config(
            deepcopy(config), self.scores_with_nulls_table, ingredient_cache=cache
        )
        cached_start = time.time()
        for i in range(COUNT):
            Shelf.from_config(
                deepcopy(config), self.scores_with_nulls_table, ingredient_cache=cache
            )
        cached_duration = time.time() - cached_start

        # usually this performance is somewhere between 100 and 1000 times faster, but
        # we should be conservative here
        if not cached_duration < (uncached_duration / 50):
            # let's just warn instead of actually failing the test suite
            warnings.warn(
                "cache was not fast enough: "
                f"cached duration {cached_duration}, "
                f"uncached duration: {uncached_duration}",
                UserWarning,
            )

    def test_broken_cache(self):
        """If the cache has corrupt data, it is ignored"""
        cache = Cache()
        self.shelf_from_yaml(
            """
            username: {kind: Dimension, field: username}
            count_star: {kind: Metric, field: count(*)}
            convertdate: {kind: Dimension, field: month(test_date)}
            """,
            self.scores_with_nulls_table,
            ingredient_cache=cache,
        )
        og_cache = deepcopy(cache)
        self.assertEqual(len(cache), 1)
        main_cache_key = list(cache.keys())[0]
        broken_ingredients = {
            k: ({"broken": "tree"}, {"broken": "validator"})
            for k in cache[main_cache_key]
        }
        cache[main_cache_key] = broken_ingredients
        self.shelf_from_yaml(
            """
            username: {kind: Dimension, field: username}
            count_star: {kind: Metric, field: count(*)}
            convertdate: {kind: Dimension, field: month(test_date)}
            """,
            self.scores_with_nulls_table,
            ingredient_cache=cache,
        )
        # the cache should be reinitialized, and should be identical to the old cache
        self.assertEqual(cache, og_cache)


class TestParsedIntellligentDates(ConfigTestBase):
    def test_is_current_year(self):
        """Test current year with a variety of spacing and capitalization"""

        data = [
            "is current year",
            "is   current\t  \t year",
            " IS  CURRENT YEAR",
            "is Current  Year  ",
        ]

        for is_current_year in data:
            shelf = self.shelf_from_yaml(
                """
test:
    kind: Metric
    field: "if(dt {}, count, 0)"
""".format(
                    is_current_year
                ),
                self.datetester_table,
            )
            recipe = self.recipe(shelf=shelf).metrics("test")
            today = date.today()
            start_dt = date(today.year, 1, 1)
            end_dt = start_dt + relativedelta(years=1, days=-1)
            self.assertRecipeSQL(
                recipe,
                f"""SELECT sum(CASE
                    WHEN (datetester.dt BETWEEN '{start_dt}' AND '{end_dt}') THEN datetester.count
                    ELSE 0
                END) AS test
                FROM datetester""",
            )
            self.assertRecipeCSV(recipe, "test\n12\n")

    def test_prior_years(self):
        """Test current year with a variety of spacing and capitalization"""

        data = ["is prior year ", "is  PREVIOUS year  ", "is Last  Year"]

        for is_prior_year in data:
            shelf = self.shelf_from_yaml(
                f"""
test:
    kind: Metric
    field: "if(dt {is_prior_year}, count, 0)"
""",
                self.datetester_table,
            )
            recipe = self.recipe(shelf=shelf).metrics("test")
            today = date.today()
            start_dt = date(today.year - 1, 1, 1)
            end_dt = start_dt + relativedelta(years=1, days=-1)
            self.assertRecipeSQL(
                recipe,
                f"""SELECT sum(CASE
                            WHEN (datetester.dt BETWEEN '{start_dt}' AND '{end_dt}') THEN datetester.count
                            ELSE 0
                        END) AS test
                FROM datetester""",
            )
            self.assertRecipeCSV(recipe, "test\n12\n")

    def test_ytd(self):
        """Test current year with a variety of spacing and capitalization"""

        data = [
            "is current ytd ",
            "is prior ytd  ",
            "is this  ytd",
            "is last ytd",
            "is next ytd",
        ]

        unique_sql = set()
        today = date.today()

        for ytd in data:
            shelf = self.shelf_from_yaml(
                f"""
test:
    kind: Metric
    field: "if(dt {ytd}, count, 0)"
""",
                self.datetester_table,
            )
            recipe = self.recipe(shelf=shelf).metrics("test")
            self.assertRecipeCSV(recipe, "test\n{}\n".format(today.month))
            unique_sql.add(recipe.to_sql())
        assert len(unique_sql) == 3

    def test_is_not(self):
        """Test current year with a variety of spacing and capitalization"""

        data = [
            "is current ytd ",
            "is prior ytd  ",
            "is this  ytd",
            "is last ytd",
            "is next ytd",
        ]

        unique_sql = set()
        today = date.today()

        for ytd in data:
            shelf = self.shelf_from_yaml(
                f"""
test:
    kind: Metric
    field: "if(not(dt {ytd}), count, 0)"
""",
                self.datetester_table,
            )
            recipe = self.recipe(shelf=shelf).metrics("test")
            self.assertRecipeCSV(recipe, f"test\n{100-today.month}\n")
            unique_sql.add(recipe.to_sql())
        assert len(unique_sql) == 3

    def test_qtr(self):
        """Quarters are always three months"""
        data = ["is current qtr", "IS PRIOR Qtr", "Is NEXT QTR"]

        unique_sql = set()
        today = date.today()

        for ytd in data:
            shelf = self.shelf_from_yaml(
                f"""
test:
    kind: Metric
    field: "if(dt {ytd}, count, 0)"
""",
                self.datetester_table,
            )
            recipe = self.recipe(shelf=shelf).metrics("test")
            self.assertRecipeCSV(recipe, "test\n3\n")
            unique_sql.add(recipe.to_sql())
        assert len(unique_sql) == 3

    def test_convert_date(self):
        """We can convert dates using formats.
        But it is better to use the date_aggregation property"""

        shelf = self.shelf_from_yaml(
            """
test:
    kind: Dimension
    field: dt
    format: "%Y"
test2:
    kind: Dimension
    field: dt
    format: "<%Y>"
test3:
    kind: Dimension
    field: dt
    format: "<%B %Y>"
test4:
    kind: Dimension
    field: dt
    format: "%B %Y"
test5:
    kind: Dimension
    field: dt
    format: ".2f"
""",
            self.datetester_table,
        )
        recipe = self.recipe(shelf=shelf).dimensions(
            "test", "test2", "test3", "test4", "test5"
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT date_trunc('year', datetester.dt) AS test,
                date_trunc('year', datetester.dt) AS test2,
                date_trunc('month', datetester.dt) AS test3,
                date_trunc('month', datetester.dt) AS test4,
                datetester.dt AS test5
            FROM datetester
            GROUP BY test,
                    test2,
                    test3,
                    test4,
                    test5""",
        )

    def test_convert_date_with_date_aggregation(self):
        """We can convert dates using date_aggregation"""

        shelf = self.shelf_from_yaml(
            """
test:
    kind: Dimension
    field: dt
    date_aggregation: year
test2:
    kind: Dimension
    field: dt
    date_aggregation: year
test3:
    kind: Dimension
    field: dt
    date_aggregation: month
test4:
    kind: Dimension
    field: dt
    date_aggregation: month
test5:
    kind: Dimension
    field: dt
    format: ".2f"
""",
            self.datetester_table,
        )
        recipe = self.recipe(shelf=shelf).dimensions(
            "test", "test2", "test3", "test4", "test5"
        )
        self.assertRecipeSQL(
            recipe,
            """SELECT date_trunc('year', datetester.dt) AS test,
                date_trunc('year', datetester.dt) AS test2,
                date_trunc('month', datetester.dt) AS test3,
                date_trunc('month', datetester.dt) AS test4,
                datetester.dt AS test5
            FROM datetester
            GROUP BY test,
                    test2,
                    test3,
                    test4,
                    test5""",
        )

    def test_invalid_date_aggregation(self):
        """Invalid date formats are ignored in a shelf construction
        and would fail ingredient validation."""

        shelf = self.shelf_from_yaml(
            """
test:
    kind: Dimension
    field: dt
    date_aggregation: years
""",
            self.datetester_table,
        )
        recipe = self.recipe(shelf=shelf).dimensions("test")
        # No date aggregation is applied.
        self.assertRecipeSQL(
            recipe,
            """SELECT datetester.dt AS test
FROM datetester
GROUP BY test""",
        )

    def test_id_fields(self):
        """We can have fields that end in _id

        _id is a protected name
        """

        shelf = self.shelf_from_yaml(
            """
student:
    kind: Dimension
    field: student
student_id:
    kind: Dimension
    field: student_id
""",
            self.id_tests_table,
        )
        recipe = self.recipe(shelf=shelf).dimensions("student")
        self.assertRecipeSQL(
            recipe,
            """SELECT id_tests.student AS student
            FROM id_tests
            GROUP BY student""",
        )
        recipe = self.recipe(shelf=shelf).dimensions("student_id")
        self.assertRecipeSQL(
            recipe,
            """SELECT id_tests.student_id AS student_id
            FROM id_tests
            GROUP BY student_id""",
        )

        recipe = self.recipe(shelf=shelf).dimensions("student", "student_id")
        self.assertRecipeSQL(
            recipe,
            """SELECT id_tests.student AS student,
                id_tests.student_id AS student_id
            FROM id_tests
            GROUP BY student,
                    student_id""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            student,student_id,student_id,student_id_id
            annika,2,annika,2
            chip,3,chip,3
            chris,1,chris,1
            """,
        )

        recipe = self.recipe(shelf=shelf).dimensions("student_id", "student")
        self.assertRecipeSQL(
            recipe,
            """SELECT id_tests.student AS student,
                id_tests.student_id AS student_id
            FROM id_tests
            GROUP BY student,
                    student_id""",
        )
        self.assertRecipeCSV(
            recipe,
            """
            student,student_id,student_id,student_id_id
            annika,2,annika,2
            chip,3,chip,3
            chris,1,chris,1
            """,
        )


class TestParsedFieldConfig(ConfigTestBase):
    """Parsed fields save the original config"""

    def test_parsed_field_config(self):
        """Test the trees generated by parsing fields"""
        shelf = self.shelf_from_yaml(
            """
convertdate:
    kind: Dimension
    field: "month(test_date)"
strings:
    kind: Dimension
    field: "string(test_date)+string(score)"
department_buckets:
    kind: Dimension
    field: department
    format: ".2f"
    buckets:
    - label: 'foosers'
      condition: '="foo"'
    - label: 'moosers'
      condition: '="moo"'
    buckets_default_label: 'others'
""",
            self.scores_with_nulls_table,
        )
        assert shelf["convertdate"].meta["_config"]["field"] == "month(test_date)"
        assert (
            shelf["strings"].meta["_config"]["field"]
            == "string(test_date)+string(score)"
        )
        assert shelf["department_buckets"].meta["_config"] == {
            "buckets": [
                {"condition": '="foo"', "label": "foosers"},
                {"condition": '="moo"', "label": "moosers"},
            ],
            "buckets_default_label": "others",
            "field": "department",
            "format": ".2f",
            "kind": "dimension",
        }
