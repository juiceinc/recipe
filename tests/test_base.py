import csv
import io
import os
from typing import Iterator, List, Optional
from unittest import TestCase

from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Table,
    distinct,
    func,
    select,
)

from recipe import Dimension, Filter, IdValueDimension, Metric, Recipe, Shelf
from recipe.dbinfo.dbinfo import get_dbinfo

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))

sqlite_db = os.path.join(ROOT_DIR, "testdata.db")

load_dotenv()


def get_bigquery_engine_kwargs():
    GOOGLE_CLOUD_PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
    GOOGLE_CLOUD_PRIVATE_KEY_ID = os.environ["GOOGLE_CLOUD_PRIVATE_KEY_ID"]
    GOOGLE_CLOUD_PRIVATE_KEY = os.environ["GOOGLE_CLOUD_PRIVATE_KEY"]
    creds = {
        "type": "service_account",
        "project_id": GOOGLE_CLOUD_PROJECT,
        "private_key_id": GOOGLE_CLOUD_PRIVATE_KEY_ID,
        "private_key": GOOGLE_CLOUD_PRIVATE_KEY,
        "client_email": "jbo-test-bigquery-admin@juicebox-open-test.iam.gserviceaccount.com",
        "client_id": "114757123849235966640",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/jbo-test-bigquery-admin%40juicebox-open-test.iam.gserviceaccount.com",
    }
    return {"credentials_info": creds}


def get_bigquery_connection_string():
    GOOGLE_CLOUD_PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]

    return f"bigquery://{GOOGLE_CLOUD_PROJECT}/recipe_test_data"


def strip_columns_from_csv(content: str, ignore_columns: Optional[List]) -> str:
    """Strip columns from a csv string"""
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
    """Dedent a string, but also strip leading and trailing newlines"""
    return "\n".join([x.lstrip() for x in s.split("\n")]).lstrip("\n").rstrip("\n")


class UtilsTestCase(TestCase):
    """Test cases for utility functions"""

    def test_strip_columns_from_csv(self):
        """Test strip_columns_from_csv"""
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
        """Test str_dedent"""
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
    """Test cases for Recipe"""

    maxDiff = None
    connection_string = f"sqlite:///{sqlite_db}"
    engine_kwargs = {}
    # connection_string = "postgresql+psycopg2://postgres:postgres@db:5432/postgres"
    create_table_kwargs = {}

    def setUp(self):
        super().setUp()
        # Set up a default shelf to use
        self.shelf = self.mytable_shelf

    def assertRecipeCSV(self, recipe: Recipe, csv_text: str, ignore_columns=None):
        """Recipe data returns the supplied csv content"""
        actual = recipe.dataset.export("csv", lineterminator=str("\n")).strip("\n")
        actual = strip_columns_from_csv(actual, ignore_columns=ignore_columns)
        expected = str_dedent(csv_text).strip("\n")
        if actual != expected:
            print(f"Actual:\n{actual}\n\nExpected:\n{expected}")

        self.assertEqual(actual, expected)

    def assertRecipeSQL(self, recipe: Recipe, sql_text: str):
        """Recipe data returns the supplied csv content"""
        if str_dedent(recipe.to_sql()) != str_dedent(sql_text):
            print(f"Actual:\n{recipe.to_sql()}\n\nExpected:\n{sql_text}")
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

    def _skip_drivername(
        self,
        include_drivernames: Optional[List[str]] = None,
        exclude_drivernames: Optional[List[str]] = None,
    ):
        drivername = self.drivername
        if include_drivernames is not None:
            return not any(drivername.startswith(dn) for dn in include_drivernames)
        if exclude_drivernames is not None:
            return any(drivername.startswith(dn) for dn in exclude_drivernames)

    def assertRecipe(
        self,
        recipe: Recipe,
        sql: Optional[str] = None,
        csv: Optional[str] = None,
        include_drivernames: Optional[List[str]] = None,
        exclude_drivernames: Optional[List[str]] = None,
    ):
        if self._skip_drivername(include_drivernames, exclude_drivernames):
            return
        if sql:
            self.assertRecipeSQL(recipe, sql)
        if csv:
            self.assertRecipeCSV(recipe, csv)

    @classmethod
    def setUpClass(cls):
        """Set up tables using connection_string. The data must be already loaded
        into the tables.
        """
        super(RecipeTestCase, cls).setUpClass()
        cls.dbinfo = get_dbinfo(cls.connection_string, **cls.engine_kwargs)
        cls.meta = cls.dbinfo.sqlalchemy_meta
        cls.session = cls.dbinfo.session_factory()
        cls.drivername = "foo"

        cls.weird_table_with_column_named_true_table = Table(
            "weird_table_with_column_named_true",
            cls.meta,
            Column("true", String),
            extend_existing=True,
        )

        cls.basic_table = Table(
            "foo",
            cls.meta,
            Column("first", String),
            Column("last", String),
            Column("age", Integer),
            Column("birth_date", Date),
            Column("dt", DateTime),
            extend_existing=True,
        )

        cls.datetester_table = Table(
            "datetester",
            cls.meta,
            Column("dt", Date),
            Column("count", Integer),
            extend_existing=True,
        )

        cls.scores_table = Table(
            "scores",
            cls.meta,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            extend_existing=True,
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
            extend_existing=True,
        )

        cls.scores_with_nulls_table = Table(
            "scores_with_nulls",
            cls.meta,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            extend_existing=True,
        )

        cls.tagscores_table = Table(
            "tagscores",
            cls.meta,
            Column("username", String),
            Column("tag", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            extend_existing=True,
        )

        cls.id_tests_table = Table(
            "id_tests",
            cls.meta,
            Column("student", String),
            Column("student_id", Integer),
            Column("age", Integer),
            Column("age_id", Integer),
            Column("score", Float),
            extend_existing=True,
        )

        cls.census_table = Table(
            "census",
            cls.meta,
            Column("state", String),
            Column("sex", String),
            Column("age", Integer),
            Column("pop2000", Integer),
            Column("pop2008", Integer),
            extend_existing=True,
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
            extend_existing=True,
        )

        cls.mytable_shelf = Shelf(
            {
                "first": Dimension(cls.basic_table.c.first),
                "last": Dimension(cls.basic_table.c.last),
                "firstlast": Dimension(
                    cls.basic_table.c.last, id_expression=cls.basic_table.c.first
                ),
                "age": Metric(func.sum(cls.basic_table.c.age)),
            }
        )

        cls.mytable_extrarole_shelf = Shelf(
            {
                "first": Dimension(cls.basic_table.c.first),
                "last": Dimension(cls.basic_table.c.last),
                "firstlastage": Dimension(
                    cls.basic_table.c.last,
                    id_expression=cls.basic_table.c.first,
                    age_expression=cls.basic_table.c.age,
                ),
                "age": Metric(func.sum(cls.basic_table.c.age)),
            }
        )

        cls.scores_shelf = Shelf(
            {
                "username": Dimension(cls.scores_table.c.username),
                "department": Dimension(
                    cls.scores_table.c.department,
                    anonymizer=lambda value: value[::-1] if value else "None",
                ),
                "testid": Dimension(cls.scores_table.c.testid),
                "test_cnt": Metric(func.count(distinct(cls.tagscores_table.c.testid))),
                "score": Metric(func.avg(cls.scores_table.c.score)),
            }
        )

        cls.tagscores_shelf = Shelf(
            {
                "username": Dimension(cls.tagscores_table.c.username),
                "department": Dimension(cls.tagscores_table.c.department),
                "testid": Dimension(cls.tagscores_table.c.testid),
                "tag": Dimension(cls.tagscores_table.c.tag),
                "test_cnt": Metric(func.count(distinct(cls.tagscores_table.c.testid))),
                "score": Metric(
                    func.avg(cls.tagscores_table.c.score), summary_aggregation=func.sum
                ),
            }
        )

        cls.census_shelf = Shelf(
            {
                "state": Dimension(cls.census_table.c.state),
                "idvalue_state": IdValueDimension(
                    cls.census_table.c.state, "State:" + cls.census_table.c.state
                ),
                "sex": Dimension(cls.census_table.c.sex),
                "age": Dimension(cls.census_table.c.age),
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
                "state": Dimension(cls.state_fact_table.c.name),
                "abbreviation": Dimension(cls.state_fact_table.c.abbreviation),
            }
        )


class TestRecipeTestCase(RecipeTestCase):
    engine_kwargs = {
        # "echo_pool": "debug",
        "pool_size": 20,
        "max_overflow": 0,
        # "pool_reset_on_return": None,
    }

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
            with self.dbinfo.connection_scope() as conn:
                res = conn.execute(select(func.count()).select_from(table)).scalar()
                print(self.dbinfo.drivername)
                self.assertEqual(res, expected_count)

    def test_strip_columns_from_csv(self):
        content = """a,b,c\n1,2,3"""
        c2 = strip_columns_from_csv(content, ignore_columns=["b"])
        self.assertEqual(c2, "a,c\n1,3")


class TestDBInfo(RecipeTestCase):
    engine_kwargs = {
        # "echo_pool": "debug",
        "pool_size": 20,
        "max_overflow": 0,
        # "pool_reset_on_return": None,
    }

    def test_select(self):
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
            with self.dbinfo.connection_scope() as conn:
                res = conn.execute(select(func.count()).select_from(table)).scalar()
                self.assertEqual(res, expected_count)

    def test_select_pool(self):
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

        # selects = [select(func.count()).select_from(table) for table, cnt in values]
        # result = self.dbinfo.run_in_pool(selects)
        # self.assertEqual(result, [])

        # for table, expected_count in values:
        #     with self.dbinfo.connection_scope() as conn:
        #         res = conn.execute(select(func.count()).select_from(table)).scalar()
        #         self.assertEqual(res, expected_count)
