"""Test Recipe against multiple database engines"""

import os
import pytest
from datetime import date, datetime

from sqlalchemy import Column, Date, DateTime, Integer, MetaData, String, Table, insert

from recipe import Recipe, Shelf, get_oven


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
