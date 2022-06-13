"""Test Recipe against multiple database engines"""

import os
import pytest
from datetime import date, datetime

from sqlalchemy import Column, Date, DateTime, Integer, MetaData, String, Table, insert

from recipe import Recipe, Shelf, get_oven
from recipe.oven.base import OvenBase

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def str_dedent(s):
    return "\n".join([x.lstrip() for x in s.split("\n")]).lstrip("\n")


@pytest.mark.skip("Can't run this without connection")
class TestPostgres(object):
    def setup(self):
        connection_string = os.environ.get("POSTGRES_CONNECTION_STR", None)
        self.skip_tests = False
        if connection_string is None:
            self.skip_tests = True
            return

        self.oven = get_oven(connection_string)

        self.meta = MetaData(bind=self.oven.engine)
        self.session = self.oven.Session()

        self.table = Table(
            "brands", self.meta, autoload=True, autoload_with=self.oven.engine
        )
        d = {"id": {"icon": "check-square", "kind": "Dimension", "field": "id"}}

        self.shelf = self.shelf_from_yaml(
            """
    id:
        kind: Dimension
        field: id
    webflow_brand_profile_id:
        kind: Dimension
        field: webflow_brand_profile_id
    """,
            self.table,
        )

    def shelf_from_yaml(self, yaml_config, selectable):
        """Create a shelf directly from configuration"""
        return Shelf.from_validated_yaml(yaml_config, selectable)

    def recipe(self, **kwargs):
        return Recipe(shelf=self.shelf, session=self.session, **kwargs)

    def testit(self):

        tables = {}
        engine = self.oven.engine
        with engine.connect() as conn:
            for schema in engine.dialect.get_schema_names(conn):
                schema_tables = engine.table_names(schema=schema, connection=conn)
                if schema_tables:
                    tables[schema] = sorted(schema_tables)

        r = self.recipe().dimensions("id")
        assert len(r.all()) == 10


def get_gcloud_credentials_obj():
    # FIXME: Load these creds from a file
    bq_creds = {}
    return bq_creds


def get_bigquery():
    """Get a BigQuery client authenticated with our credentials."""
    google_creds = get_gcloud_credentials_obj()
    return bigquery.Client(credentials=google_creds, project=google_creds.project_id)


class BQOven:
    def __init__(self, connection_string=None):
        self.engine = self.init_engine()
        self.Session = sessionmaker(bind=self.engine)

    def init_engine(self, *args, **kwargs):
        conn_string = "bigquery://juicebox-open-test/juicebox"
        engine_kwargs = {"credentials_info": get_gcloud_credentials_obj()}
        return create_engine(conn_string, **engine_kwargs)


@pytest.mark.skip("Can't run this witout connection")
class TestBigQuery(object):
    def setup(self):
        self.oven = BQOven()
        self.meta = MetaData(bind=self.oven.engine)
        self.session = self.oven.Session()

        self.table = Table(
            "runningwithtimestamp_8599_b3310438d33748de",
            self.meta,
            autoload=True,
            autoload_with=self.oven.engine,
        )
        d = {"id": {"icon": "check-square", "kind": "Dimension", "field": "id"}}

        self.shelf = self.shelf_from_yaml(
            """
    date:
        kind: Dimension
        field: day(date)
    name:
        kind: Dimension
        field: name
    distance:
        kind: Metric
        field: sum(distance)

    """,
            self.table,
        )

    def shelf_from_yaml(self, yaml_config, selectable):
        """Create a shelf directly from configuration"""
        return Shelf.from_validated_yaml(yaml_config, selectable)

    def recipe(self, **kwargs):
        return Recipe(shelf=self.shelf, session=self.session, **kwargs)

    def test_query(self):
        from datetime import datetime

        d1 = datetime(2020, 1, 1)
        d2 = datetime(2020, 12, 31)
        f = self.shelf["date"].build_filter(
            operator="between", value=["2020-01-01", "2020-12-31"]
        )
        f2 = self.shelf["date"].build_filter(operator="between", value=[d1, d2])
        for c in self.table.columns:
            print(c, c.type)
        print(f)
        print(f2)
        recipe = self.recipe().dimensions("date").metrics("distance").filters(f2)
        print(recipe.to_sql())

        for row in recipe.all():
            print(row)
        # assert 1 == 2

    def testit(self):
        print(self.table)
        for c in self.table.columns:
            print(c, c.type)
        assert 1 == 1


@pytest.mark.skip("Can't run this witout connection")
class TestRecipeSQLServer(object):
    def setup(self):
        connection_string = os.environ.get("SQL_SERVER_CONNECTION_STR", None)
        self.skip_tests = False
        if connection_string is None:
            self.skip_tests = True
            return

        self.oven = get_oven(connection_string)

        self.meta = MetaData(bind=self.oven.engine)
        self.session = self.oven.Session()

        self.table = Table(
            "foo",
            self.meta,
            Column("first", String),
            Column("last", String),
            Column("age", Integer),
            Column("birth_date", Date),
            Column("dt", DateTime),
            extend_existing=True,
        )
        self.meta.create_all(self.oven.engine)

        data = [
            {
                "first": "hi",
                "last": "there",
                "age": 5,
                "birth_date": date(2015, 1, 1),
                "dt": datetime(2005, 12, 1, 12, 15),
            },
            {
                "first": "hi",
                "last": "fred",
                "age": 10,
                "birth_date": date(2015, 5, 15),
                "dt": datetime(2013, 10, 15, 5, 20, 10),
            },
        ]
        with self.oven.engine.connect() as conn:
            for row in data:
                conn.execute(insert(self.table).values(**row))

        self.shelf = self.shelf_from_yaml(
            """
first:
    kind: Dimension
    field: first
last:
    kind: Dimension
    field: last
firstlast:
    kind: Dimension
    field: "first + last"
    id_field: first
age:
    kind: Measure
    field: sum(age)
test_month:
    kind: Dimension
    field: month(birth_date)
year_by_format:
    kind: Dimension
    field: dt
    format: "%Y"
count:
    kind: Measure
    field: count(*)
""",
            self.table,
        )

    def teardown(self):
        if not self.skip_tests:
            self.meta.drop_all(self.oven.engine)

    def shelf_from_yaml(self, yaml_config, selectable):
        """Create a shelf directly from configuration"""
        return Shelf.from_validated_yaml(yaml_config, selectable)

    def recipe(self, **kwargs):
        return Recipe(shelf=self.shelf, session=self.session, **kwargs)

    def assertRecipeCSV(self, recipe, content):
        actual = recipe.dataset.csv.replace("\r\n", "\n")
        expected = str_dedent(content)
        assert actual == expected

    def test_dimension(self):
        if self.skip_tests:
            return
        recipe = self.recipe().metrics("age", "count").dimensions("first")
        self.assertRecipeCSV(
            recipe,
            """
            first,age,count,first_id
            hi,15,2,hi
            """,
        )
        recipe = self.recipe().metrics("age").dimensions("firstlast")
        self.assertRecipeCSV(
            recipe,
            """
            firstlast_id,firstlast,age,firstlast_id
            hi,hifred,10,hi
            hi,hithere,5,hi
            """,
        )

    def test_dates_and_Datetimes(self):
        """We can convert dates using formats"""
        if self.skip_tests:
            return
        recipe = (
            self.recipe()
            .dimensions("year_by_format")
            .metrics("count")
            .order_by("year_by_format")
        )
        self.assertRecipeCSV(
            recipe,
            """
            year_by_format,count,year_by_format_id
            2005-01-01 00:00:00,1,2005-01-01 00:00:00
            2013-01-01 00:00:00,1,2013-01-01 00:00:00
            """,
        )
        recipe = (
            self.recipe()
            .dimensions("year_by_format")
            .metrics("count")
            .order_by("-year_by_format")
        )
        self.assertRecipeCSV(
            recipe,
            """
            year_by_format,count,year_by_format_id
            2013-01-01 00:00:00,1,2013-01-01 00:00:00
            2005-01-01 00:00:00,1,2005-01-01 00:00:00
            """,
        )

        # Test a month() conversion
        recipe = (
            self.recipe()
            .dimensions("test_month")
            .metrics("age", "count")
            .order_by("-test_month")
        )
        self.assertRecipeCSV(
            recipe,
            """
            test_month,age,count,test_month_id
            2015-05-01,10,1,2015-05-01
            2015-01-01,5,1,2015-01-01
            """,
        )
