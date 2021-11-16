"""Test Recipe against multiple database engines"""

from copy import copy
from datetime import date, datetime

import pytest
from dateutil.relativedelta import relativedelta
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Table,
    distinct,
    func,
    join,
    MetaData,
    insert,
)
from sqlalchemy.ext.declarative import declarative_base
from sureberus.schema import Boolean
from yaml import safe_load

from recipe import Dimension, Metric, Recipe, Shelf, get_oven
from recipe import oven
from recipe.ingredients import Having



class TestRecipeIngredients(object):
    def setup(self):
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

        self.shelf = Shelf(
            {
                "first": Dimension(self.table.c.first, group_by_strategy="direct"),
                "last": Dimension(self.table.c.last, group_by_strategy="direct"),
                "firstlast": Dimension(
                    self.table.c.last,
                    id_expression=self.table.c.first,
                    group_by_strategy="direct",
                ),
                "age": Metric(func.sum(self.table.c.age)),
                "count": Metric(func.count("*")),
            }
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
                result = conn.execute(insert(self.table).values(**row))

    def shelf_from_yaml(self, yaml_config, selectable):
        """Create a shelf directly from configuration"""
        return Shelf.from_validated_yaml(yaml_config, selectable)

    def teardown(self):
        self.meta.drop_all(self.oven.engine)

    #     def recipe(self, **kwargs):
    #         return Recipe(shelf=self.shelf, session=self.session, **kwargs)

    #     def test_dimension(self):
    #         recipe = self.recipe().metrics("age","count").dimensions("first")
    #         assert recipe.all()[0].first == "hi"
    #         assert recipe.all()[0].age == 15
    #         assert recipe.all()[0].count == 2
    #         assert recipe.stats.rows == 1

    #     def test_idvaluedimension(self):
    #         recipe = self.recipe().metrics("age").dimensions("firstlast")
    #         assert recipe.all()[0].firstlast == "fred"
    #         assert recipe.all()[0].firstlast_id == "hi"
    #         assert recipe.all()[0].age == 10
    #         assert recipe.all()[1].firstlast == "there"
    #         assert recipe.all()[1].firstlast_id == "hi"
    #         assert recipe.all()[1].age == 5
    #         assert recipe.stats.rows == 2

    #     def test_having(self):

    #         hv = Having(func.sum(self.table.c.age) < 10)
    #         recipe = (
    #             self.recipe()
    #             .metrics("age")
    #             .dimensions("last")
    #             .filters(self.table.c.age > 2)
    #             .filters(hv)
    #             .order_by("last")
    #         )
    #         assert (
    #             recipe.dataset.csv.replace("\r\n", "\n")
    #             == """last,age,last_id
    # there,5,there
    # """
    #         )

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
count:
    kind: Measure
    field: count(*)
""",
            self.table,
        )
        recipe = Recipe(shelf=shelf, session=self.session).dimensions("test").metrics("count")
        print(recipe.to_sql())
        print(recipe.all())
        assert recipe.all()[0].test == datetime(2005, 12, 1, 12, 15)
        # assert recipe.all()[0].test5 == datetime(2005, 1, 1)
