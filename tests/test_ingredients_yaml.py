"""
Test recipes built from yaml files in the ingredients directory.

"""

import os

from datetime import date
import pytest
from tests.test_base import Census, MyTable, oven, ScoresWithNulls

from recipe import AutomaticFilters, BadIngredient, Recipe, Shelf


class TestRecipeIngredientsYaml(object):
    def setup(self):
        self.session = oven.Session()

    def assert_recipe_csv(self, recipe, csv_text):
        assert recipe.dataset.export("csv", lineterminator="\n") == csv_text

    def validated_shelf(self, shelf_name, table):
        """Load a file from the sample ingredients.yaml files."""
        d = os.path.dirname(os.path.realpath(__file__))
        fn = os.path.join(d, "ingredients", shelf_name)
        contents = open(fn).read()
        return Shelf.from_validated_yaml(contents, table)

    def unvalidated_shelf(self, shelf_name, table):
        """Load a file from the sample ingredients.yaml files."""
        d = os.path.dirname(os.path.realpath(__file__))
        fn = os.path.join(d, "ingredients", shelf_name)
        contents = open(fn).read()
        return Shelf.from_yaml(contents, table)

    def test_ingredients1_from_validated_yaml(self):
        shelf = self.validated_shelf("ingredients1.yaml", MyTable)
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
        shelf = self.validated_shelf("ingredients1.yaml", MyTable)
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
        shelf = self.validated_shelf("ingredients1.yaml", MyTable)
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
        shelf = self.validated_shelf("census.yaml", Census)
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
        shelf = self.unvalidated_shelf("census.yaml", Census)
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
        """Build a recipe that depends on the results of another recipe """
        shelf = self.validated_shelf("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        nested_shelf = self.validated_shelf("census_nested.yaml", recipe)
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
   ORDER BY census.state) AS anon_1"""
        )
        self.assert_recipe_csv(
            nested_recipe,
            """num_states,ttlpop
2,13117719
""",
        )

    def test_nested_census_from_yaml(self):
        shelf = self.unvalidated_shelf("census.yaml", Census)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("pop2000", "ttlpop")
            .order_by("state")
        )
        nested_shelf = self.unvalidated_shelf("census_nested.yaml", recipe)
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
        shelf = self.unvalidated_shelf("census.yaml", Census)
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
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY age_buckets"""
        )
        self.assert_recipe_csv(
            recipe,
            """age_buckets,pop2000,age_buckets_id
babies,164043,babies
children,948240,children
oldsters,4567879,oldsters
teens,614548,teens
""",
        )

    def test_census_condition_between(self):
        shelf = self.unvalidated_shelf("census.yaml", Census)
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
        shelf = self.unvalidated_shelf("census.yaml", Census)
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
        shelf = self.unvalidated_shelf("census.yaml", Census)
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
           WHEN (census.age < 13) THEN 'children'
           WHEN (census.age < 20) THEN 'teens'
           ELSE 'oldsters'
       END AS mixed_buckets,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY mixed_buckets
ORDER BY CASE
             WHEN (census.state IN ('Vermont',
                                    'New Hampshire')) THEN 0
             WHEN (census.age < 2) THEN 1
             WHEN (census.age < 13) THEN 2
             WHEN (census.age < 20) THEN 3
             ELSE 9999
         END DESC"""
        )
        self.assert_recipe_csv(
            recipe,
            """mixed_buckets,pop2000,mixed_buckets_id
oldsters,4124620,oldsters
teens,550515,teens
children,859206,children
babies,150889,babies
northeast,609480,northeast
""",
        )

    def test_census_buckets_ordering(self):
        shelf = self.unvalidated_shelf("census.yaml", Census)
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
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY age_buckets
ORDER BY CASE
             WHEN (census.age < 2) THEN 0
             WHEN (census.age < 13) THEN 1
             WHEN (census.age < 20) THEN 2
             ELSE 9999
         END"""
        )
        self.assert_recipe_csv(
            recipe,
            """age_buckets,pop2000,age_buckets_id
babies,164043,babies
children,948240,children
teens,614548,teens
oldsters,4567879,oldsters
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
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY age_buckets
ORDER BY CASE
             WHEN (census.age < 2) THEN 0
             WHEN (census.age < 13) THEN 1
             WHEN (census.age < 20) THEN 2
             ELSE 9999
         END DESC"""
        )
        self.assert_recipe_csv(
            recipe,
            """age_buckets,pop2000,age_buckets_id
oldsters,4567879,oldsters
teens,614548,teens
children,948240,children
babies,164043,babies
""",
        )

    def test_complex_census_from_validated_yaml(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics """
        shelf = self.validated_shelf("census_complex.yaml", Census)
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
ORDER BY census.state"""
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
        metrics """
        shelf = self.validated_shelf("census_complex.yaml", Census)
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

    def test_complex_census_quickselect_from_validated_yaml(self):
        """Build a recipe that uses complex definitions dimensions and
        metrics and quickselects"""
        shelf = self.validated_shelf("census_complex.yaml", Census)
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
ORDER BY census.state"""
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
ORDER BY census.state"""
        )
        self.assert_recipe_csv(
            recipe,
            """state_raw,pop2008,state,state_id
Vermont,620602,The Green Mountain State,Vermont
""",
        )

    def test_shelf_with_references(self):
        """Build a recipe using a shelf that uses field references """
        shelf = self.validated_shelf("census_references.yaml", Census)
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
ORDER BY census.state"""
        )  # noqa: E501
        self.assert_recipe_csv(
            recipe,
            """state_raw,popdivide,state,state_id
Tennessee,0.3856763995010324,The Volunteer State,Tennessee
Vermont,0.4374284968466095,The Green Mountain State,Vermont
""",
        )

    def test_shelf_with_condition_references(self):
        """Build a recipe using a shelf that uses condition references """
        shelf = self.validated_shelf("census_references.yaml", Census)
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
ORDER BY census.state"""
        )  # noqa: E501
        self.assert_recipe_csv(
            recipe,
            """state_raw,pop2008oldsters,state,state_id
Tennessee,2821955,The Volunteer State,Tennessee
Vermont,311842,The Green Mountain State,Vermont
""",
        )

    def test_bad_census_from_validated_yaml(self):
        """ Test a bad yaml file """
        with pytest.raises(Exception):
            self.validated_shelf("census_bad.yaml", Census)

    def test_bad_census_from_yaml(self):
        """ Test a bad yaml file """
        with pytest.raises(BadIngredient):
            self.unvalidated_shelf("census_bad.yaml", Census)

    def test_bad_census_in_from_validated_yaml(self):
        """ Test a bad yaml file """
        with pytest.raises(Exception):
            self.validated_shelf("census_bad_in.yaml", Census)

    def test_bad_census_in_from_yaml(self):
        """ Test a bad yaml file """
        with pytest.raises(BadIngredient):
            self.unvalidated_shelf("census_bad_in.yaml", Census)

    def test_deprecated_ingredients_dividemetric(self):
        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.validated_shelf("census_deprecated.yaml", Census)

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
ORDER BY census.state"""
        )  # noqa: E501
        self.assert_recipe_csv(
            recipe,
            """state,popchg,state_id
Tennessee,0.9166167263773563,Tennessee
Vermont,0.9820786913351858,Vermont
""",
        )

    def test_deprecated_ingredients_lookupdimension(self):
        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.validated_shelf("census_deprecated.yaml", Census)

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
ORDER BY census.state"""
        )  # noqa: E501
        self.assert_recipe_csv(
            recipe,
            """state_characteristic_raw,pop2000,state_characteristic,state_characteristic_id
Tennessee,5685230,Volunteery,Tennessee
Vermont,609480,Taciturny,Vermont
""",
        )

    def test_deprecated_ingredients_idvaluedimension(self):
        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.validated_shelf("census_deprecated.yaml", Census)

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
ORDER BY census.state,
         census.pop2000
LIMIT 5
OFFSET 0"""
        )  # noqa: E501
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

    def test_deprecated_ingredients_wtdavgmetric(self):
        """ Test deprecated ingredient kinds in a yaml file """
        shelf = self.validated_shelf("census_deprecated.yaml", Census)

        # We can IdValueDimension
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("state")
            .metrics("avgage")
            .order_by("state_idval")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state,
       CAST(sum(census.age * census.pop2000) AS FLOAT) / (coalesce(CAST(sum(census.pop2000) AS FLOAT), 0.0) + 1e-09) AS avgage
FROM census
GROUP BY state
ORDER BY census.state,
         census.pop2000"""
        )  # noqa: E501

        self.assert_recipe_csv(
            recipe,
            """state,avgage,state_id
Tennessee,36.24667550829078,Tennessee
Vermont,37.0597968760254,Vermont
""",
        )


class TestNullHandling(TestRecipeIngredientsYaml):
    def test_dimension_null_handling(self):
        """ Test different ways of handling nulls in dimensions """
        shelf = self.validated_shelf("scores_with_nulls.yaml", ScoresWithNulls)

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
ORDER BY scores_with_nulls.department"""
        )  # noqa: E501

        self.assert_recipe_csv(
            recipe,
            """department,score,department_id
,80.0,
ops,90.0,ops
sales,,sales
""",
        )

    def test_dimension_null_handling_with_lookup_default(self):
        """ Test different ways of handling nulls in dimensions """

        # This uses the lookup_default to replace items that aren't found in lookup
        # department_lookup:
        #     kind: Dimension
        #     field: department
        #     lookup:
        #         sales: Sales
        #         ops: Operations
        #     lookup_default: Unknown

        shelf = self.validated_shelf("scores_with_nulls.yaml", ScoresWithNulls)
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
ORDER BY scores_with_nulls.department"""
        )  # noqa: E501

        self.assert_recipe_csv(
            recipe,
            """department_lookup_raw,score,department_lookup,department_lookup_id
,80.0,Unknown,
ops,90.0,Operations,ops
sales,,Sales,sales
""",
        )

    def test_dimension_null_handling_with_null_in_lookup(self):
        """ Test different ways of handling nulls in dimensions """

        # This uses a null in the lookup
        # department_lookup_with_null:
        #     kind: Dimension
        #     field: department
        #     lookup:
        #         sales: Sales
        #         ops: Operations
        #         null: 'can not find department'

        shelf = self.validated_shelf("scores_with_nulls.yaml", ScoresWithNulls)
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
ORDER BY scores_with_nulls.department"""
        )  # noqa: E501

        self.assert_recipe_csv(
            recipe,
            """department_lookup_with_null_raw,score,department_lookup_with_null,department_lookup_with_null_id
,80.0,can not find department,
ops,90.0,Operations,ops
sales,,Sales,sales
""",
        )

    def test_dimension_null_handling_with_default(self):
        """ Test different ways of handling nulls in dimensions """

        # This uses default which will coalesce missing values to a defined value
        # department_default:
        #     kind: Dimension
        #     field:
        #         value: department
        #         default: 'N/A'
        shelf = self.validated_shelf("scores_with_nulls.yaml", ScoresWithNulls)
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
ORDER BY coalesce(scores_with_nulls.department, 'N/A')"""
        )  # noqa: E501

        self.assert_recipe_csv(
            recipe,
            """department_default,score,department_default_id
N/A,80.0,N/A
ops,90.0,ops
sales,,sales
""",
        )

    def test_dimension_null_handling_with_buckets(self):
        """ Test different ways of handling nulls in dimensions """

        # This uses default which will coalesce missing values to a defined value
        # department_default:
        #     kind: Dimension
        #     field:
        #         value: department
        #         default: 'N/A'
        shelf = self.validated_shelf("scores_with_nulls.yaml", ScoresWithNulls)
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
       avg(scores_with_nulls.score) AS score
FROM scores_with_nulls
GROUP BY department_buckets
ORDER BY CASE
             WHEN (scores_with_nulls.department = 'sales') THEN 0
             WHEN (scores_with_nulls.department = 'ops') THEN 1
             ELSE 9999
         END"""
        )  # noqa: E501

        self.assert_recipe_csv(
            recipe,
            """department_buckets,score,department_buckets_id
Sales,,Sales
Operations,90.0,Operations
Other,80.0,Other
""",
        )

    def test_dimension_null_handling_multi_approaches(self):
        """ Test different ways of handling nulls in dimensions """

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
        shelf = self.validated_shelf("scores_with_nulls.yaml", ScoresWithNulls)
        recipe = (
            Recipe(shelf=shelf, session=self.session)
            .dimensions("department_lookup_with_everything")
            .metrics("score")
            .order_by("department_lookup_with_everything")
        )
        assert (
            recipe.to_sql()
            == """SELECT coalesce(scores_with_nulls.department, 'N/A') AS department_lookup_with_everything_raw,
       avg(scores_with_nulls.score) AS score
FROM scores_with_nulls
GROUP BY department_lookup_with_everything_raw
ORDER BY coalesce(scores_with_nulls.department, 'N/A')"""
        )  # noqa: E501

        self.assert_recipe_csv(
            recipe,
            """department_lookup_with_everything_raw,score,department_lookup_with_everything,department_lookup_with_everything_id
N/A,80.0,Unknown,N/A
ops,90.0,Operations,ops
sales,,Sales,sales
""",
        )

    def test_metric_null_handling(self):
        """ Test handling nulls in metrics """
        shelf = self.validated_shelf("scores_with_nulls.yaml", ScoresWithNulls)

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
       coalesce(avg(scores_with_nulls.score), -1.0) AS score_with_default
FROM scores_with_nulls
GROUP BY department
ORDER BY scores_with_nulls.department"""
        )  # noqa: E501

        self.assert_recipe_csv(
            recipe,
            """department,score_with_default,department_id
,80.0,
ops,90.0,ops
sales,-1.0,sales
""",
        )
