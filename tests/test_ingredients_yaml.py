"""
Test recipes built from yaml files in the ingredients directory.

"""

import os

from datetime import date
from dateutil.relativedelta import relativedelta
import pytest
from tests.test_base import (
    Census,
    MyTable,
    oven,
    ScoresWithNulls,
    DateTester,
    WeirdTableWithColumnNamedTrue,
)

from recipe import (
    AutomaticFilters,
    BadIngredient,
    InvalidIngredient,
    Recipe,
    Shelf,
    BadRecipe,
    InvalidColumnError,
)


class ConfigTestBase(object):
    """A base class for testing shelves built from v1 or v2 config."""

    # The directory to look for yaml config files
    yaml_location = "ingredients"
    shelf_cache = {}

    def setup(self):
        self.session = oven.Session()

    def assert_recipe_csv(self, recipe, csv_text):
        assert recipe.dataset.export("csv", lineterminator=str("\n")) == csv_text

    def shelf_from_filename(self, shelf_name, selectable=None):
        """Load a file from the sample ingredients.yaml files."""
        d = os.path.dirname(os.path.realpath(__file__))
        fn = os.path.join(d, self.yaml_location, shelf_name)
        contents = open(fn).read()
        return self.shelf_from_yaml(contents, selectable)

    def shelf_from_yaml(self, yaml_config, selectable):
        """Create a shelf directly from configuration"""
        return Shelf.from_validated_yaml(yaml_config, selectable)


class TestRecipeIngredientsYaml(ConfigTestBase):
    def test_ingredients1_from_validated_yaml(self):
        shelf = self.shelf_from_filename("ingredients1.yaml", MyTable)
        recipe = (
            Recipe(shelf=shelf, session=self.session).metrics("age").dimensions("first")
        )
        self.assert_recipe_csv(
            recipe,
            """first,age,first_id
hi,15,hi
""",
        )

    def test_ingredients1_from_yaml(self):
        shelf = self.shelf_from_filename("ingredients1.yaml", MyTable)
        recipe = (
            Recipe(shelf=shelf, session=self.session).metrics("age").dimensions("first")
        )
        self.assert_recipe_csv(
            recipe,
            """first,age,first_id
hi,15,hi
""",
        )

    def test_ingredients1_between_dates(self):
        shelf = self.shelf_from_filename("ingredients1.yaml", MyTable)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("date_between")
        today = date.today()
        assert (
            recipe.to_sql()
            == """SELECT sum(CASE
               WHEN (foo.birth_date BETWEEN '{}' AND '{}') THEN foo.age
           END) AS date_between
FROM foo""".format(
                date(today.year - 20, today.month, today.day), today
            )
        )
        self.assert_recipe_csv(
            recipe,
            """date_between
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
        recipe = Recipe(shelf=shelf, session=self.session).metrics("dt_between")
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
        self.assert_recipe_csv(
            recipe,
            """dt_between
15
""",
        )

    def test_census_from_validated_yaml(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        self.assert_recipe_csv(
            recipe,
            """state,pop2000,ttlpop,state_id
Tennessee,5685230,11887637,Tennessee
Vermont,609480,1230082,Vermont
""",
        )

    def test_census_from_yaml(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        self.assert_recipe_csv(
            recipe,
            """state,pop2000,ttlpop,state_id
Tennessee,5685230,11887637,Tennessee
Vermont,609480,1230082,Vermont
""",
        )

    def test_nested_census_from_validated_yaml(self):
        """Build a recipe that depends on the results of another recipe"""
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        nested_shelf = self.shelf_from_filename("census_nested.yaml", recipe)
        nested_recipe = Recipe(shelf=nested_shelf, session=self.session).metrics(
            "ttlpop", "num_states"
        )
        assert (
            nested_recipe.to_sql()
            == """SELECT count(anon_1.state) AS num_states,
       sum(anon_1.ttlpop) AS ttlpop
FROM
  (SELECT census.state AS state,
          sum(census.pop2000) AS pop2000,
          sum(census.pop2000 + census.pop2008) AS ttlpop
   FROM census
   GROUP BY state
   ORDER BY state) AS anon_1"""
        )
        self.assert_recipe_csv(
            nested_recipe,
            """num_states,ttlpop
2,13117719
""",
        )

    def test_nested_census_from_yaml(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        nested_shelf = self.shelf_from_filename("census_nested.yaml", recipe)
        nested_recipe = Recipe(shelf=nested_shelf, session=self.session).metrics(
            "ttlpop", "num_states"
        )
        self.assert_recipe_csv(
            nested_recipe,
            """num_states,ttlpop
2,13117719
""",
        )

    def test_census_buckets(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("age_buckets")
            .metrics("pop2000")
        )
        assert (
            recipe.to_sql()
            == """SELECT CASE
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
         age_buckets"""
        )
        self.assert_recipe_csv(
            recipe,
            """age_buckets,age_buckets_order_by,pop2000,age_buckets_id
babies,0,164043,babies
children,1,948240,children
teens,2,614548,teens
oldsters,9999,4567879,oldsters
""",
        )

    def test_census_condition_between(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("teenagers")
        assert (
            recipe.to_sql()
            == """SELECT sum(CASE
               WHEN (census.age BETWEEN 13 AND 19) THEN census.pop2000
           END) AS teenagers
FROM census"""
        )
        self.assert_recipe_csv(
            recipe,
            """teenagers
614548
""",
        )

    def test_census_condition_between_dates(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = Recipe(shelf=shelf, session=self.session).metrics("teenagers")
        assert (
            recipe.to_sql()
            == """SELECT sum(CASE
               WHEN (census.age BETWEEN 13 AND 19) THEN census.pop2000
           END) AS teenagers
FROM census"""
        )
        self.assert_recipe_csv(
            recipe,
            """teenagers
614548
""",
        )

    def test_census_mixed_buckets(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("mixed_buckets")
            .metrics("pop2000")
            .order_by("-mixed_buckets")
        )
        assert (
            recipe.to_sql()
            == """SELECT CASE
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
         mixed_buckets DESC"""
        )

        self.assert_recipe_csv(
            recipe,
            """mixed_buckets,mixed_buckets_order_by,pop2000,mixed_buckets_id
oldsters,9999,4124620,oldsters
teens,3,550515,teens
children,2,859206,children
babies,1,150889,babies
northeast,0,609480,northeast
""",
        )

    def test_census_buckets_ordering(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("age_buckets")
            .metrics("pop2000")
            .order_by("age_buckets")
        )
        assert (
            recipe.to_sql()
            == """SELECT CASE
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
         age_buckets"""
        )
        self.assert_recipe_csv(
            recipe,
            """age_buckets,age_buckets_order_by,pop2000,age_buckets_id
babies,0,164043,babies
children,1,948240,children
teens,2,614548,teens
oldsters,9999,4567879,oldsters
""",
        )
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("age_buckets")
            .metrics("pop2000")
            .order_by("-age_buckets")
        )
        assert (
            recipe.to_sql()
            == """SELECT CASE
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
         age_buckets DESC"""
        )
        self.assert_recipe_csv(
            recipe,
            """age_buckets,age_buckets_order_by,pop2000,age_buckets_id
oldsters,9999,4567879,oldsters
teens,2,614548,teens
children,1,948240,children
babies,0,164043,babies
""",
        )

    def test_census_buckets_nolabel(self):
        """If not default label is provided, buckets default to "Not found" """
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("age_buckets_nolabel")
            .metrics("pop2000")
            .order_by("age_buckets_nolabel")
        )
        assert (
            recipe.to_sql()
            == """SELECT CASE
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
         age_buckets_nolabel"""
        )
        self.assert_recipe_csv(
            recipe,
            """age_buckets_nolabel,age_buckets_nolabel_order_by,pop2000,age_buckets_nolabel_id
babies,0,164043,babies
children,1,948240,children
teens,2,614548,teens
Not found,9999,4567879,Not found
""",
        )

    def test_complex_census_from_validated_yaml(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics"""
        shelf = self.shelf_from_filename("census_complex.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("pop2000")
            .order_by("state")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_raw,
       sum(CASE
               WHEN (census.age > 40) THEN census.pop2000
           END) AS pop2000
FROM census
GROUP BY state_raw
ORDER BY state_raw"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_raw,pop2000,state,state_id
Tennessee,2392122,The Volunteer State,Tennessee
Vermont,271469,The Green Mountain State,Vermont
""",
        )

    def test_complex_census_from_validated_yaml_math(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics"""
        shelf = self.shelf_from_filename("census_complex.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("allthemath")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_raw,
       sum((((census.pop2000 + census.pop2008) - census.pop2000) * census.pop2008) / (coalesce(CAST(census.pop2000 AS FLOAT), 0.0) + 1e-09)) AS allthemath
FROM census
GROUP BY state_raw"""
        )  # noqa: E501
        self.assert_recipe_csv(
            recipe,
            """state_raw,allthemath,state,state_id
Tennessee,6873286.452931551,The Volunteer State,Tennessee
Vermont,660135.4074068918,The Green Mountain State,Vermont
""",
        )

    def test_complex_census_quickselect_from_validated_yaml(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics and quickselects"""
        shelf = self.shelf_from_filename("census_complex.yaml", Census)
        recipe = (
            Recipe(
                shelf=shelf, session=self.session, extension_classes=(AutomaticFilters,)
            )
            .dimensions("state")
            .metrics("pop2008")
            .order_by("state")
            .automatic_filters({"state__quickselect": "younger"})
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_raw,
       sum(census.pop2008) AS pop2008
FROM census
WHERE census.age < 40
GROUP BY state_raw
ORDER BY state_raw"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_raw,pop2008,state,state_id
Tennessee,3297299,The Volunteer State,Tennessee
Vermont,300605,The Green Mountain State,Vermont
""",
        )
        recipe = (
            Recipe(
                shelf=shelf, session=self.session, extension_classes=(AutomaticFilters,)
            )
            .dimensions("state")
            .metrics("pop2008")
            .order_by("state")
            .automatic_filters({"state__quickselect": "vermontier"})
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_raw,
       sum(census.pop2008) AS pop2008
FROM census
WHERE census.state = 'Vermont'
GROUP BY state_raw
ORDER BY state_raw"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_raw,pop2008,state,state_id
Vermont,620602,The Green Mountain State,Vermont
""",
        )

    def test_shelf_with_references(self):
        """Build a recipe using a shelf that uses field references"""
        shelf = self.shelf_from_filename("census_references.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("popdivide")
            .order_by("state")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_raw,
       CAST(sum(CASE
                    WHEN (census.age > 40) THEN census.pop2000
                END) AS FLOAT) / (coalesce(CAST(sum(census.pop2008) AS FLOAT), 0.0) + 1e-09) AS popdivide
FROM census
GROUP BY state_raw
ORDER BY state_raw"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_raw,popdivide,state,state_id
Tennessee,0.3856763995010324,The Volunteer State,Tennessee
Vermont,0.4374284968466095,The Green Mountain State,Vermont
""",
        )

    def test_shelf_with_invalidcolumn(self):
        """Build a recipe using a shelf that uses field references"""
        shelf = self.shelf_from_filename("census_references.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("badfield")
            .order_by("state")
        )
        with pytest.raises(InvalidColumnError):
            recipe.to_sql()

    def test_shelf_with_condition_references(self):
        """Build a recipe using a shelf that uses condition references"""
        shelf = self.shelf_from_filename("census_references.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("pop2008oldsters")
            .order_by("state")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_raw,
       sum(CASE
               WHEN (census.age > 40) THEN census.pop2008
           END) AS pop2008oldsters
FROM census
GROUP BY state_raw
ORDER BY state_raw"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_raw,pop2008oldsters,state,state_id
Tennessee,2821955,The Volunteer State,Tennessee
Vermont,311842,The Green Mountain State,Vermont
""",
        )

    def test_bad_census(self):
        """Test a bad yaml file"""
        with pytest.raises(Exception):
            self.shelf_from_filename("census_bad.yaml", Census)

    def test_bad_census_in(self):
        """Test a bad yaml file"""
        with pytest.raises(Exception):
            self.shelf_from_filename("census_bad_in.yaml", Census)

    def test_deprecated_ingredients_dividemetric(self):
        """Test deprecated ingredient kinds in a yaml file"""
        shelf = self.shelf_from_filename("census_deprecated.yaml", Census)

        # We can DivideMetric
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("popchg")
            .order_by("state")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state,
       CAST(sum(census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2008) AS FLOAT), 0.0) + 1e-09) AS popchg
FROM census
GROUP BY state
ORDER BY state"""
        )
        self.assert_recipe_csv(
            recipe,
            """state,popchg,state_id
Tennessee,0.9166167263773563,Tennessee
Vermont,0.9820786913351858,Vermont
""",
        )

    def test_deprecated_ingredients_lookupdimension(self):
        """Test deprecated ingredient kinds in a yaml file"""
        shelf = self.shelf_from_filename("census_deprecated.yaml", Census)

        # We can LookupDimension
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state_characteristic")
            .metrics("pop2000")
            .order_by("state_characteristic")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_characteristic_raw,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY state_characteristic_raw
ORDER BY state_characteristic_raw"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_characteristic_raw,pop2000,state_characteristic,state_characteristic_id
Tennessee,5685230,Volunteery,Tennessee
Vermont,609480,Taciturny,Vermont
""",
        )

        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.shelf_from_filename("census_deprecated.yaml", Census)

        # We can IdValueDimension
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state_idval")
            .metrics("pop2000")
            .order_by("state_idval")
            .limit(5)
        )
        assert (
            recipe.to_sql()
            == """SELECT census.pop2000 AS state_idval_id,
       census.state AS state_idval,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY state_idval_id,
         state_idval
ORDER BY state_idval,
         state_idval_id
LIMIT 5
OFFSET 0"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_idval_id,state_idval,pop2000,state_idval_id
5033,Tennessee,5033,5033
5562,Tennessee,5562,5562
6452,Tennessee,6452,6452
7322,Tennessee,7322,7322
8598,Tennessee,8598,8598
""",
        )

    def test_deprecated_ingredients_idvaluedim(self):
        """Test deprecated ingredient kinds in a yaml file"""
        shelf = self.shelf_from_filename("census_deprecated.yaml", Census)

        # We can IdValueDimension
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("avgage")
            .order_by("state_idval")
        )
        # Can't order_by something that isn't used as a dimension or metric
        with pytest.raises(BadRecipe):
            recipe.to_sql()

    def test_deprecated_ingredients_idvaluedim(self):
        """Test deprecated ingredient kinds in a yaml file"""
        shelf = self.shelf_from_filename("census_deprecated.yaml", Census)

        # We can IdValueDimension
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state_idval")
            .metrics("avgage")
            .order_by("state_idval")
            .limit(10)
        )
        assert (
            recipe.to_sql()
            == """SELECT census.pop2000 AS state_idval_id,
       census.state AS state_idval,
       CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS avgage
FROM census
GROUP BY state_idval_id,
         state_idval
ORDER BY state_idval,
         state_idval_id
LIMIT 10
OFFSET 0"""
        )

        self.assert_recipe_csv(
            recipe,
            """state_idval_id,state_idval,avgage,state_idval_id
5033,Tennessee,83.9999999999833,5033
5562,Tennessee,82.99999999998506,5562
6452,Tennessee,81.99999999998728,6452
7322,Tennessee,80.99999999998893,7322
8598,Tennessee,79.99999999999069,8598
9583,Tennessee,78.99999999999176,9583
10501,Tennessee,83.999999999992,10501
10672,Tennessee,77.99999999999268,10672
11141,Tennessee,82.99999999999255,11141
11168,Tennessee,76.99999999999311,11168
""",
        )


class TestNullHandling(ConfigTestBase):
    yaml_location = "parsed_ingredients"

    def test_dimension_null_handling(self):
        """Test different ways of handling nulls in dimensions"""
        shelf = self.shelf_from_filename("scores_with_nulls.yaml", ScoresWithNulls)

        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("department")
            .metrics("score")
            .order_by("department")
        )
        assert (
            recipe.to_sql()
            == """SELECT scores_with_nulls.department AS department,
       avg(scores_with_nulls.score) AS score
FROM scores_with_nulls
GROUP BY department
ORDER BY department"""
        )

        self.assert_recipe_csv(
            recipe,
            """department,score,department_id
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

        shelf = self.shelf_from_filename("scores_with_nulls.yaml", ScoresWithNulls)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("department_lookup")
            .metrics("score")
            .order_by("department_lookup")
        )
        assert (
            recipe.to_sql()
            == """SELECT scores_with_nulls.department AS department_lookup_raw,
       avg(scores_with_nulls.score) AS score
FROM scores_with_nulls
GROUP BY department_lookup_raw
ORDER BY department_lookup_raw"""
        )

        self.assert_recipe_csv(
            recipe,
            """department_lookup_raw,score,department_lookup,department_lookup_id
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

        shelf = self.shelf_from_filename("scores_with_nulls.yaml", ScoresWithNulls)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("department_lookup_with_null")
            .metrics("score")
            .order_by("department_lookup_with_null")
        )
        assert (
            recipe.to_sql()
            == """SELECT scores_with_nulls.department AS department_lookup_with_null_raw,
       avg(scores_with_nulls.score) AS score
FROM scores_with_nulls
GROUP BY department_lookup_with_null_raw
ORDER BY department_lookup_with_null_raw"""
        )

        self.assert_recipe_csv(
            recipe,
            """department_lookup_with_null_raw,score,department_lookup_with_null,department_lookup_with_null_id
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
        shelf = self.shelf_from_filename("scores_with_nulls.yaml", ScoresWithNulls)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("department_default")
            .metrics("score")
            .order_by("department_default")
        )
        assert (
            recipe.to_sql()
            == """SELECT coalesce(scores_with_nulls.department, 'N/A') AS department_default,
       avg(scores_with_nulls.score) AS score
FROM scores_with_nulls
GROUP BY department_default
ORDER BY department_default"""
        )

        self.assert_recipe_csv(
            recipe,
            """department_default,score,department_default_id
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
        shelf = self.shelf_from_filename("scores_with_nulls.yaml", ScoresWithNulls)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("department_buckets")
            .metrics("score")
            .order_by("department_buckets")
        )
        assert (
            recipe.to_sql()
            == """SELECT CASE
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
         department_buckets"""
        )

        self.assert_recipe_csv(
            recipe,
            """department_buckets,department_buckets_order_by,score,department_buckets_id
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
        shelf = self.shelf_from_filename("scores_with_nulls.yaml", ScoresWithNulls)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("department_lookup_with_everything")
            .metrics("score")
            .order_by("-department_lookup_with_everything")
        )
        assert (
            recipe.to_sql()
            == """SELECT coalesce(scores_with_nulls.department, 'N/A') AS department_lookup_with_everything_raw,
       avg(scores_with_nulls.score) AS score
FROM scores_with_nulls
GROUP BY department_lookup_with_everything_raw
ORDER BY department_lookup_with_everything_raw DESC"""
        )

        self.assert_recipe_csv(
            recipe,
            """department_lookup_with_everything_raw,score,department_lookup_with_everything,department_lookup_with_everything_id
sales,,Sales,sales
ops,90.0,Operations,ops
N/A,80.0,Unknown,N/A
""",
        )

    def test_metric_null_handling(self):
        """Test handling nulls in metrics"""
        shelf = self.shelf_from_filename("scores_with_nulls.yaml", ScoresWithNulls)

        # score_with_default:
        #     kind: Metric
        #     field:
        #         aggregation: avg
        #         value: score
        #         default: -1.0
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("department")
            .metrics("score_with_default")
            .order_by("department")
        )
        assert (
            recipe.to_sql()
            == """SELECT scores_with_nulls.department AS department,
       avg(coalesce(scores_with_nulls.score, -1.0)) AS score_with_default
FROM scores_with_nulls
GROUP BY department
ORDER BY department"""
        )

        self.assert_recipe_csv(
            recipe,
            """department,score_with_default,department_id
,39.5,
ops,59.666666666666664,ops
sales,-1.0,sales
""",
        )


class TestRecipeIngredientsYamlParsed(TestRecipeIngredientsYaml):
    yaml_location = "parsed_ingredients"

    def test_bad_census(self):
        """Test a bad yaml file"""
        shelf = self.shelf_from_filename("census_bad.yaml", Census)
        assert isinstance(shelf["pop2000"], InvalidIngredient)
        assert (
            "No terminal defined for '>'" in shelf["pop2000"].error["extra"]["details"]
        )

    def test_bad_census_in(self):
        """Test a bad yaml file"""
        shelf = self.shelf_from_filename("census_bad_in.yaml", Census)
        assert isinstance(shelf["pop2000"], InvalidIngredient)
        assert (
            "No terminal defined for 'c'" in shelf["pop2000"].error["extra"]["details"]
        )

    def test_shelf_with_invalidcolumn(self):
        """Build a recipe using a shelf that uses field references"""
        shelf = self.shelf_from_filename("census_references.yaml", Census)
        assert isinstance(shelf["badfield"], InvalidIngredient)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("badfield")
            .order_by("state")
        )
        with pytest.raises(BadIngredient):
            recipe.to_sql()

    def test_census_buckets(self):
        shelf = self.shelf_from_filename("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("age_buckets")
            .metrics("pop2000")
        )
        assert (
            recipe.to_sql()
            == """SELECT CASE
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
         age_buckets"""
        )
        self.assert_recipe_csv(
            recipe,
            """age_buckets,age_buckets_order_by,pop2000,age_buckets_id
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
        shelf = self.shelf_from_filename("census_references.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("popdivide")
            .order_by("state")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_raw,
       CASE
           WHEN (sum(census.pop2008) = 0) THEN NULL
           ELSE CAST(sum(CASE
                             WHEN (census.age > 40) THEN census.pop2000
                         END) AS FLOAT) / CAST(sum(census.pop2008) AS FLOAT)
       END AS popdivide
FROM census
GROUP BY state_raw
ORDER BY state_raw"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_raw,popdivide,state,state_id
Tennessee,0.38567639950103244,The Volunteer State,Tennessee
Vermont,0.4374284968466102,The Green Mountain State,Vermont
""",
        )

    def test_shelf_with_invalidingredient(self):
        """Build a recipe using a shelf that uses field references"""
        shelf = self.shelf_from_filename("census.yaml", Census)
        assert isinstance(shelf["baddim"], InvalidIngredient)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("baddim")
            .metrics("pop2000")
        )
        # Trying to run the recipe raises an exception with the bad ingredient details
        with pytest.raises(BadIngredient):
            recipe.to_sql()

    def test_complex_census_from_validated_yaml_math(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics"""
        shelf = self.shelf_from_filename("census_complex.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("allthemath")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state_raw,
       sum(census.pop2000 + (census.pop2008 - census.pop2000 * CASE
                                                                   WHEN (census.pop2000 = 0) THEN NULL
                                                                   ELSE CAST(census.pop2008 AS FLOAT) / CAST(census.pop2000 AS FLOAT)
                                                               END)) AS allthemath
FROM census
GROUP BY state_raw"""
        )  # noqa: E501
        self.assert_recipe_csv(
            recipe,
            """state_raw,allthemath,state,state_id
Tennessee,5685230.0,The Volunteer State,Tennessee
Vermont,609480.0,The Green Mountain State,Vermont
""",
        )

    def test_deprecated_ingredients_idvaluedim(self):
        """Test deprecated ingredient kinds in a yaml file"""
        shelf = self.shelf_from_filename("census_deprecated.yaml", Census)

        # We can IdValueDimension
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state_idval")
            .metrics("avgage")
            .order_by("state_idval")
            .limit(10)
        )
        assert (
            recipe.to_sql()
            == """SELECT census.pop2000 AS state_idval_id,
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
OFFSET 0"""
        )

        # Parsed shelves provide better division
        self.assert_recipe_csv(
            recipe,
            """state_idval_id,state_idval,avgage,state_idval_id
5033,Tennessee,84.0,5033
5562,Tennessee,83.0,5562
6452,Tennessee,82.0,6452
7322,Tennessee,81.0,7322
8598,Tennessee,80.0,8598
9583,Tennessee,79.0,9583
10501,Tennessee,84.0,10501
10672,Tennessee,78.0,10672
11141,Tennessee,83.0,11141
11168,Tennessee,77.0,11168
""",
        )

    def test_is(self):
        """Test fields using is"""
        shelf = self.shelf_from_filename("ingredients1.yaml", MyTable)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .metrics("dt_test")
            .dimensions("first")
        )
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(CASE
               WHEN (foo.birth_date IS NULL) THEN foo.age
               ELSE 1
           END) AS dt_test
FROM foo
GROUP BY first"""
        )
        self.assert_recipe_csv(
            recipe,
            """first,dt_test,first_id
hi,2,hi
""",
        )

    def test_intelligent_date(self):
        """Test intelligent dates like `date is last year`"""
        shelf = self.shelf_from_filename("ingredients1.yaml", MyTable)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .metrics("intelligent_date_test")
            .dimensions("first")
        )
        from datetime import date
        from dateutil.relativedelta import relativedelta

        today = date.today()
        start_dt = date(today.year - 1, 1, 1)
        end_dt = start_dt + relativedelta(years=1, days=-1)
        assert (
            recipe.to_sql()
            == """SELECT foo.first AS first,
       sum(CASE
               WHEN (foo.birth_date BETWEEN '{}' AND '{}') THEN foo.age
               ELSE 2
           END) AS intelligent_date_test
FROM foo
GROUP BY first""".format(
                start_dt, end_dt
            )
        )
        self.assert_recipe_csv(
            recipe,
            """first,intelligent_date_test,first_id
hi,4,hi
""",
        )


class TestParsedSQLGeneration(ConfigTestBase):
    """More tests of SQL generation on complex parsed expressions"""

    def test_weird_table_with_column_named_true(self):
        shelf = self.shelf_from_yaml(
            """
_version: 2
"true":
    kind: Dimension
    field: "[true]"
            """,
            WeirdTableWithColumnNamedTrue,
        )

        recipe = Recipe(shelf=shelf, session=self.session).dimensions("true")
        assert (
            recipe.to_sql()
            == """SELECT weird_table_with_column_named_true."true" AS "true"
FROM weird_table_with_column_named_true
GROUP BY "true"
            """.strip()
        )

    def test_complex_field(self):
        """Test parsed field definitions that use math, field references and more"""
        shelf = self.shelf_from_yaml(
            """
_version: 2
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
            ScoresWithNulls,
        )
        recipe = Recipe(shelf=shelf, session=self.session).metrics("count_star")
        assert (
            recipe.to_sql()
            == """SELECT count(*) AS count_star
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "count_star\n6\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("total_nulls")
        assert (
            recipe.to_sql()
            == """SELECT count(DISTINCT CASE
                          WHEN (scores_with_nulls.score IS NULL) THEN scores_with_nulls.username
                      END) AS total_nulls
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "total_nulls\n3\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("chip_nulls")
        assert (
            recipe.to_sql()
            == """SELECT sum(CASE
               WHEN (scores_with_nulls.score IS NULL
                     AND scores_with_nulls.username = 'chip') THEN 1
               ELSE 0
           END) AS chip_nulls
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "chip_nulls\n1\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("chip_or_nulls")
        assert (
            recipe.to_sql()
            == """SELECT sum(CASE
               WHEN (scores_with_nulls.score IS NULL
                     OR scores_with_nulls.username = 'chip') THEN 1
               ELSE 0
           END) AS chip_or_nulls
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "chip_or_nulls\n5\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("user_null_counter")
        assert (
            recipe.to_sql()
            == """SELECT sum(CASE
               WHEN (scores_with_nulls.username IS NULL) THEN 1
               ELSE 0
           END) AS user_null_counter
FROM scores_with_nulls"""
        )

        self.assert_recipe_csv(recipe, "user_null_counter\n0\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("simple_math")
        assert (
            recipe.to_sql()
            == """SELECT count(*) + count(DISTINCT CASE
                                     WHEN (scores_with_nulls.score IS NULL) THEN scores_with_nulls.username
                                 END) + sum(CASE
                                                WHEN (scores_with_nulls.score IS NULL
                                                      AND scores_with_nulls.username = 'chip') THEN 1
                                                ELSE 0
                                            END) AS simple_math
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "simple_math\n10\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("refs_division")
        assert (
            recipe.to_sql()
            == """SELECT CAST(count(*) AS FLOAT) / 100.0 AS refs_division
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "refs_division\n0.06\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("refs_as_denom")
        assert (
            recipe.to_sql()
            == """SELECT CASE
           WHEN (count(*) = 0) THEN NULL
           ELSE 12 / CAST(count(*) AS FLOAT)
       END AS refs_as_denom
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "refs_as_denom\n2.0\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("math")
        assert (
            recipe.to_sql()
            == """SELECT CASE
           WHEN (count(*) = 0) THEN NULL
           ELSE CAST(count(*) AS FLOAT) / CAST(count(*) AS FLOAT)
       END + 2.5 AS math
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "math\n3.5\n")

        recipe = Recipe(shelf=shelf, session=self.session).metrics("parentheses")
        assert (
            recipe.to_sql()
            == """SELECT CASE
           WHEN (count(*) + 6.0 = 0) THEN NULL
           ELSE CAST(count(*) AS FLOAT) / CAST(count(*) + 6.0 AS FLOAT)
       END AS parentheses
FROM scores_with_nulls"""
        )
        self.assert_recipe_csv(recipe, "parentheses\n0.5\n")

        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("convertdate")
            .metrics("count_star")
        )
        assert (
            recipe.to_sql()
            == """SELECT date_trunc('month', scores_with_nulls.test_date) AS convertdate,
       count(*) AS count_star
FROM scores_with_nulls
GROUP BY convertdate"""
        )
        # Can't run this against sqlite so we don't test csv

        recipe = Recipe(shelf=shelf, session=self.session).dimensions("strings")
        assert (
            recipe.to_sql()
            == """SELECT CAST(scores_with_nulls.test_date AS VARCHAR) || CAST(scores_with_nulls.score AS VARCHAR) AS strings
FROM scores_with_nulls
GROUP BY strings"""
        )
        self.assert_recipe_csv(
            recipe,
            """strings,strings_id
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
_version: 2
username:
    kind: Dimension
    field: username
count_star:
    kind: Metric
    field: "count(*)"
""",
            ScoresWithNulls,
        )
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("username")
            .metrics("count_star")
        )
        assert (
            recipe.to_sql()
            == """SELECT scores_with_nulls.username AS username,
       count(*) AS count_star
FROM scores_with_nulls
GROUP BY username"""
        )
        self.assert_recipe_csv(
            recipe,
            """username,count_star,username_id
annika,2,annika
chip,3,chip
chris,1,chris
""",
        )

        # Build a recipe using the first recipe
        shelf2 = self.shelf_from_yaml(
            """
_version: 2
count_star:
    kind: Metric
    field: "count(*)"
count_username:
    kind: Metric
    field: "count(username)"
""",
            recipe,
        )
        recipe2 = Recipe(shelf=shelf2, session=self.session).metrics(
            "count_star", "count_username"
        )
        assert (
            recipe2.to_sql()
            == """SELECT count(*) AS count_star,
       count(anon_1.username) AS count_username
FROM
  (SELECT scores_with_nulls.username AS username,
          count(*) AS count_star
   FROM scores_with_nulls
   GROUP BY username) AS anon_1"""
        )
        self.assert_recipe_csv(recipe2, "count_star,count_username\n3,3\n")


class TestParsedIntellligentDates(ConfigTestBase):
    default_selectable = DateTester

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
_version: 2
test:
    kind: Metric
    field: "if(dt {}, count, 0)"
""".format(
                    is_current_year
                ),
                DateTester,
            )
            recipe = Recipe(shelf=shelf, session=self.session).metrics("test")
            today = date.today()
            start_dt = date(today.year, 1, 1)
            end_dt = start_dt + relativedelta(years=1, days=-1)
            assert (
                recipe.to_sql()
                == """SELECT sum(CASE
               WHEN (datetester.dt BETWEEN '{}' AND '{}') THEN datetester.count
               ELSE 0
           END) AS test
FROM datetester""".format(
                    start_dt, end_dt
                )
            )
            self.assert_recipe_csv(recipe, "test\n12\n")

    def test_prior_years(self):
        """Test current year with a variety of spacing and capitalization"""

        data = ["is prior year ", "is  PREVIOUS year  ", "is Last  Year"]

        for is_prior_year in data:
            shelf = self.shelf_from_yaml(
                """
_version: 2
test:
    kind: Metric
    field: "if(dt {}, count, 0)"
""".format(
                    is_prior_year
                ),
                DateTester,
            )
            recipe = Recipe(shelf=shelf, session=self.session).metrics("test")
            today = date.today()
            start_dt = date(today.year - 1, 1, 1)
            end_dt = start_dt + relativedelta(years=1, days=-1)
            assert (
                recipe.to_sql()
                == """SELECT sum(CASE
               WHEN (datetester.dt BETWEEN '{}' AND '{}') THEN datetester.count
               ELSE 0
           END) AS test
FROM datetester""".format(
                    start_dt, end_dt
                )
            )
            self.assert_recipe_csv(recipe, "test\n12\n")

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
                """
_version: 2
test:
    kind: Metric
    field: "if(dt {}, count, 0)"
""".format(
                    ytd
                ),
                DateTester,
            )
            recipe = Recipe(shelf=shelf, session=self.session).metrics("test")
            self.assert_recipe_csv(recipe, "test\n{}\n".format(today.month))
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
                """
_version: 2
test:
    kind: Metric
    field: "if(not(dt {}), count, 0)"
""".format(
                    ytd
                ),
                DateTester,
            )
            recipe = Recipe(shelf=shelf, session=self.session).metrics("test")
            self.assert_recipe_csv(recipe, "test\n{}\n".format(100 - today.month))
            unique_sql.add(recipe.to_sql())
        assert len(unique_sql) == 3

    def test_qtr(self):
        """Quarters are always three months"""
        data = ["is current qtr", "IS PRIOR Qtr", "Is NEXT QTR"]

        unique_sql = set()
        today = date.today()

        for ytd in data:
            shelf = self.shelf_from_yaml(
                """
_version: 2
test:
    kind: Metric
    field: "if(dt {}, count, 0)"
""".format(
                    ytd
                ),
                DateTester,
            )
            recipe = Recipe(shelf=shelf, session=self.session).metrics("test")
            self.assert_recipe_csv(recipe, "test\n3\n")
            unique_sql.add(recipe.to_sql())
        assert len(unique_sql) == 3

    def test_convert_date(self):
        """We can convert dates using formats"""

        shelf = self.shelf_from_yaml(
            """
_version: 2
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
            DateTester,
        )
        recipe = Recipe(shelf=shelf, session=self.session).dimensions(
            "test", "test2", "test3", "test4", "test5"
        )
        assert (
            recipe.to_sql()
            == """SELECT date_trunc('year', datetester.dt) AS test,
       date_trunc('year', datetester.dt) AS test2,
       date_trunc('month', datetester.dt) AS test3,
       date_trunc('month', datetester.dt) AS test4,
       datetester.dt AS test5
FROM datetester
GROUP BY test,
         test2,
         test3,
         test4,
         test5"""
        )


class TestParsedFieldConfig(ConfigTestBase):
    """Parsed fields save the original config"""

    def test_parsed_field_config(self):
        """Test the trees generated by parsing fields"""
        shelf = self.shelf_from_yaml(
            """
_version: 2
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
            ScoresWithNulls,
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
