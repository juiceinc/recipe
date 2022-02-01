import csv
import io
import os
from datetime import date
from typing import Iterator
from unittest import TestCase
from typing import Union
from dateutil.relativedelta import relativedelta
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    distinct,
    func,
    select,
)
from yaml import safe_load

from recipe import Dimension, Filter, IdValueDimension, Metric, Recipe, Shelf, get_oven


def strip_columns_from_csv(content: str, ignore_columns: list = None) -> str:
    if ignore_columns:
        content = str_dedent(content)
        rows = list(csv.DictReader(content.splitlines()))
        for row in rows:
            for col in ignore_columns:
                row.pop(col, None)
        if rows:
            csv_content = io.StringIO()
            first_row = rows[0]
            writer = csv.DictWriter(csv_content, fieldnames=list(first_row.keys()))
            writer.writeheader()
            writer.writerows(rows)
            return csv_content.getvalue().replace("\r\n", "\n").strip("\n")

    return content


def str_dedent(s: str) -> str:
    return "\n".join([x.lstrip() for x in s.split("\n")]).lstrip("\n").rstrip("\n")


class UtilsTestCase(TestCase):
    def test_strip_columns_from_csv(self):
        csv_content = """
            a,b,foo,c
            1,2,3,cow
            5,6,7,cookie
        """
        self.assertEqual(
            strip_columns_from_csv(csv_content, ignore_columns=["foo"]),
            "a,b,c\n1,2,cow\n5,6,cookie",
        )
        self.assertEqual(
            strip_columns_from_csv(csv_content, ignore_columns=[None]),
            "a,b,foo,c\n1,2,3,cow\n5,6,7,cookie",
        )
        self.assertEqual(
            strip_columns_from_csv(csv_content, ignore_columns="abc"), "foo\n3\n7"
        )

    def test_str_dedent(self):
        csv_content = """
            a,b,foo,c
            1,2,3,cow
            5,6,7,cookie
        """
        self.assertEqual(str_dedent(csv_content), "a,b,foo,c\n1,2,3,cow\n5,6,7,cookie")

        csv_content = """


            a,b,foo,c
            1,2,3,cow
            5,6,7,cookie


        """
        self.assertEqual(str_dedent(csv_content), "a,b,foo,c\n1,2,3,cow\n5,6,7,cookie")
        csv_content = """a,b,foo,c
            1,2,3,cow
        5,6,7,cookie


        """
        self.assertEqual(str_dedent(csv_content), "a,b,foo,c\n1,2,3,cow\n5,6,7,cookie")

        csv_content = """


            a,b,foo,c

            1,2,3,cow
            5,6,7,cookie


        """
        self.assertEqual(
            str_dedent(csv_content), "a,b,foo,c\n\n1,2,3,cow\n5,6,7,cookie"
        )


class RecipeTestCase(TestCase):
    """Test cases that can build and test a recipe"""

    maxDiff = None
    connection_string = "sqlite://"
    create_table_kwargs = {}
    # When testing SQL server, we need to use these two options
    # create_table_kwargs = {"schema": "demo"}
    # create_tables = False

    def setUp(self):
        super().setUp()
        # Set up a default shelf to use
        self.shelf = self.mytable_shelf

    def assertRecipeCSV(
        self, recipe: Recipe, csv_text: Union[str, list], ignore_columns=None
    ):
        """Recipe data returns the supplied csv content"""
        actual = strip_columns_from_csv(
            recipe.dataset.export("csv", lineterminator=str("\n")).strip("\n"),
            ignore_columns=ignore_columns,
        )
        if isinstance(csv_text, str):
            expected = str_dedent(csv_text).strip("\n")
        else:
            expected = [str_dedent(_).strip("\n") for _ in csv_text]

        if isinstance(expected, str):
            if actual != expected:
                print(f"Actual:\n{actual}\n\nExpected:\n{expected}")
            self.assertEqual(actual, expected)
        else:
            if actual not in expected:
                print(f"Actual:\n{actual}\n\nExpected:\n{expected}")
            self.assertTrue(actual in expected)

    def assertRecipeSQL(self, recipe: Recipe, sql_text: str):
        """Recipe data returns the supplied csv content"""
        if str_dedent(recipe.to_sql()) != str_dedent(sql_text):
            print(f"Actual:\n{recipe.to_sql()}\n\nExpected:\n{sql_text}")
        if self.connection_string.startswith("sqlite"):
            self.assertEqual(str_dedent(recipe.to_sql()), str_dedent(sql_text))

    def assertRecipeSQLContains(self, recipe: Recipe, contains_sql_text: str):
        """Recipe data returns the supplied csv content"""
        self.assertTrue(str_dedent(contains_sql_text) in str_dedent(recipe.to_sql()))

    def assertRecipeSQLNotContains(self, recipe: Recipe, contains_sql_text: str):
        """Recipe data returns the supplied csv content"""
        self.assertTrue(
            str_dedent(contains_sql_text) not in str_dedent(recipe.to_sql())
        )

    def recipe_list(self, *args, **kwargs) -> Iterator[Recipe]:
        for potential_recipe in args:
            if isinstance(potential_recipe, Recipe):
                yield potential_recipe
            elif isinstance(potential_recipe, dict):
                yield self.recipe_from_config(potential_recipe, **kwargs)

    def recipe(self, **kwargs):
        """Construct a recipe."""
        recipe_args = {
            "shelf": getattr(self, "shelf", Shelf()),
            "session": self.session,
            "extension_classes": getattr(self, "extension_classes", []),
        }
        recipe_args.update(kwargs)
        return Recipe(**recipe_args)

    def recipe_from_config(self, config: dict, **kwargs):
        """Construct a recipe from a configuration dict."""
        recipe_args = {
            "shelf": getattr(self, "shelf", Shelf()),
            "session": self.session,
            "extension_classes": getattr(self, "extension_classes", []),
        }
        recipe_args.update(kwargs)
        shelf = recipe_args.pop("shelf")

        return Recipe.from_config(shelf, config, **recipe_args)

    @classmethod
    def load_data(cls, table_name):
        """Load data from the data/ directory if the table doesn't already have data"""
        table = getattr(cls, table_name)
        data = safe_load(open(os.path.join(cls.root_dir, "data", f"{table_name}.yml")))
        cls.oven.engine.execute(table.insert(), data)

    @classmethod
    def setUpClass(cls):
        """Set up tables using a connection_string to define an oven.

        Tables are loaded using data in data/{tablename}.yml
        """
        super(RecipeTestCase, cls).setUpClass()
        cls.oven = get_oven(cls.connection_string)
        cls.meta = MetaData(bind=cls.oven.engine)
        cls.session = cls.oven.Session()

        cls.root_dir = os.path.abspath(os.path.dirname(__file__))

        cls.weird_table_with_column_named_true_table = Table(
            "weird_table_with_column_named_true", cls.meta, Column("true", String)
        )

        cls.basic_table = Table(
            "foo",
            cls.meta,
            Column("first", String),
            Column("last", String),
            Column("age", Integer),
            Column("birth_date", Date),
            Column("dt", DateTime),
            **cls.create_table_kwargs,
        )

        cls.datetester_table = Table(
            "datetester", cls.meta, Column("dt", Date), Column("count", Integer)
        )

        cls.scores_table = Table(
            "scores",
            cls.meta,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            **cls.create_table_kwargs,
        )

        cls.datatypes_table = Table(
            "datatypes",
            cls.meta,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            Column("test_datetime", DateTime),
            Column("valid_score", Boolean),
            **cls.create_table_kwargs,
        )

        cls.scores_with_nulls_table = Table(
            "scores_with_nulls",
            cls.meta,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            **cls.create_table_kwargs,
        )

        cls.tagscores_table = Table(
            "tagscores",
            cls.meta,
            Column("username", String),
            Column("tag", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            **cls.create_table_kwargs,
        )

        cls.id_tests_table = Table(
            "id_tests",
            cls.meta,
            Column("student", String),
            Column("student_id", Integer),
            Column("age", Integer),
            Column("age_id", Integer),
            Column("score", Float),
            **cls.create_table_kwargs,
        )

        cls.census_table = Table(
            "census",
            cls.meta,
            Column("state", String),
            Column("sex", String),
            Column("age", Integer),
            Column("pop2000", Integer),
            Column("pop2008", Integer),
            **cls.create_table_kwargs,
        )

        cls.state_fact_table = Table(
            "state_fact",
            cls.meta,
            Column("id", String),
            Column("name", String),
            Column("abbreviation", String),
            Column("sort", String),
            Column("status", String),
            Column("occupied", String),
            Column("notes", String),
            Column("fips_state", String),
            Column("assoc_press", String),
            Column("standard_federal_region", String),
            Column("census_region", String),
            Column("census_region_name", String),
            Column("census_division", String),
            Column("census_division_name", String),
            Column("circuit_court", String),
            **cls.create_table_kwargs,
        )

        if getattr(cls, "create_tables", True):
            cls.meta.drop_all(cls.oven.engine)
            cls.meta.create_all(cls.oven.engine)
            cls.load_data("weird_table_with_column_named_true_table")
            cls.load_data("basic_table")
            cls.load_data("scores_table")
            cls.load_data("datatypes_table")
            cls.load_data("scores_with_nulls_table")
            cls.load_data("tagscores_table")
            cls.load_data("id_tests_table")
            cls.load_data("census_table")
            cls.load_data("state_fact_table")

            # Load the datetester_table with dynamic date data
            start_dt = date(date.today().year, date.today().month, 1)
            data = [
                {"dt": start_dt + relativedelta(months=offset_month), "count": 1}
                for offset_month in range(-50, 50)
            ]
            cls.oven.engine.execute(cls.datetester_table.insert(), data)

        if cls.connection_string.startswith("mssql"):
            # SQLServer can not use aliases in group bys and also
            # does not support date/time conversions due to an issue with pyodbc
            # parameters in queries
            # https://github.com/mkleehammer/pyodbc/issues/479
            default_group_by_strategy = "direct"
        else:
            default_group_by_strategy = "labels"
        dim_kwargs = {"group_by_strategy": default_group_by_strategy}
        cls.dim_kwargs = dim_kwargs

        cls.mytable_shelf = Shelf(
            {
                "first": Dimension(cls.basic_table.c.first, **dim_kwargs),
                "last": Dimension(cls.basic_table.c.last, **dim_kwargs),
                "firstlast": Dimension(
                    cls.basic_table.c.last,
                    id_expression=cls.basic_table.c.first,
                    **dim_kwargs,
                ),
                "age": Metric(func.sum(cls.basic_table.c.age)),
            }
        )

        cls.mytable_extrarole_shelf = Shelf(
            {
                "first": Dimension(cls.basic_table.c.first, **dim_kwargs),
                "last": Dimension(cls.basic_table.c.last, **dim_kwargs),
                "firstlastage": Dimension(
                    cls.basic_table.c.last,
                    id_expression=cls.basic_table.c.first,
                    age_expression=cls.basic_table.c.age,
                    **dim_kwargs,
                ),
                "age": Metric(func.sum(cls.basic_table.c.age)),
            }
        )

        cls.scores_shelf = Shelf(
            {
                "username": Dimension(cls.scores_table.c.username, **dim_kwargs),
                "department": Dimension(
                    cls.scores_table.c.department,
                    anonymizer=lambda value: value[::-1] if value else "None",
                    **dim_kwargs,
                ),
                "testid": Dimension(cls.scores_table.c.testid, **dim_kwargs),
                "test_cnt": Metric(func.count(distinct(cls.tagscores_table.c.testid))),
                "score": Metric(func.avg(cls.scores_table.c.score)),
            }
        )

        cls.tagscores_shelf = Shelf(
            {
                "username": Dimension(cls.tagscores_table.c.username, **dim_kwargs),
                "department": Dimension(cls.tagscores_table.c.department, **dim_kwargs),
                "testid": Dimension(cls.tagscores_table.c.testid, **dim_kwargs),
                "tag": Dimension(cls.tagscores_table.c.tag, **dim_kwargs),
                "test_cnt": Metric(func.count(distinct(cls.tagscores_table.c.testid))),
                "score": Metric(
                    func.avg(cls.tagscores_table.c.score), summary_aggregation=func.sum
                ),
            }
        )

        cls.census_shelf = Shelf(
            {
                "state": Dimension(cls.census_table.c.state, **dim_kwargs),
                "idvalue_state": IdValueDimension(
                    cls.census_table.c.state,
                    "State:" + cls.census_table.c.state,
                    **dim_kwargs,
                ),
                "sex": Dimension(cls.census_table.c.sex, **dim_kwargs),
                "age": Dimension(cls.census_table.c.age, **dim_kwargs),
                "pop2000": Metric(func.sum(cls.census_table.c.pop2000)),
                "pop2000_sum": Metric(
                    func.sum(cls.census_table.c.pop2000), summary_aggregation=func.sum
                ),
                "pop2008": Metric(func.sum(cls.census_table.c.pop2008)),
                "filter_all": Filter(1 == 0),
            }
        )

        cls.statefact_shelf = Shelf(
            {
                "state": Dimension(cls.state_fact_table.c.name, **dim_kwargs),
                "abbreviation": Dimension(cls.state_fact_table.c.abbreviation),
            }
        )

    def test_sample_data_loaded(self):
        values = [
            (self.weird_table_with_column_named_true_table, 2),
            (self.basic_table, 2),
            (self.scores_table, 6),
            (self.datatypes_table, 6),
            (self.scores_with_nulls_table, 6),
            (self.tagscores_table, 10),
            (self.id_tests_table, 5),
            (self.census_table, 344),
            (self.state_fact_table, 2),
            (self.datetester_table, 100),
        ]

        for table, expected_count in values:
            self.assertEqual(
                self.session.execute(
                    select([func.count()]).select_from(table)
                ).scalar(),
                expected_count,
            )
