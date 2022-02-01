"""Test Recipe against multiple database engines"""

import os
import pytest
from datetime import date, datetime

from sqlalchemy import Column, Date, DateTime, Integer, MetaData, String, Table, insert

from recipe import Recipe, Shelf, get_oven
from tests.test_base import RecipeTestCase


SQL_SERVER_CONNECTION_STR = os.environ.get("SQL_SERVER_CONNECTION_STR")


@pytest.mark.skip("Can't run this witout connection")
class TestRecipeSQLServer(RecipeTestCase):
    connection_string = SQL_SERVER_CONNECTION_STR

    def setUp(self):
        self.shelf = self.shelf_from_yaml(
            """
_version: 2
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
            self.datatypes_table,
        )

    def shelf_from_yaml(self, yaml_config, selectable):
        """Create a shelf directly from configuration"""
        return Shelf.from_validated_yaml(yaml_config, selectable)

    def test_dimension(self):
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
