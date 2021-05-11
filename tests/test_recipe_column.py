from recipe.schemas.lark_grammar import SQLAlchemyBuilder
from recipe.column import DimensionColumn, MetricColumn
from unittest import TestCase
from sqlalchemy import func

from .test_base import DataTypeser


# class DataTypeser(Base):
#     username = Column("username", String(), primary_key=True)
#     department = Column("department", String())
#     testid = Column("testid", String())
#     score = Column("score", Float())
#     test_date = Column("test_date", Date())
#     test_datetime = Column("test_datetime", DateTime())

#     __tablename__ = "datatypes"
#     __table_args__ = {"extend_existing": True}


class TestRecipeColumn(TestCase):
    def test_dimension_column_datatypes(self):
        c = DimensionColumn("foo", DataTypeser.department)
        self.assertEqual(c.datatype, "str")

        c = DimensionColumn("foo", DataTypeser.score)
        self.assertEqual(c.datatype, "num")

        c = DimensionColumn("foo", DataTypeser.department + DataTypeser.testid)
        self.assertEqual(c.datatype, "str")

        c = DimensionColumn("foo", DataTypeser.department < "A")
        self.assertEqual(c.datatype, "bool")

        c = DimensionColumn("foo", DataTypeser.test_date)
        self.assertEqual(c.datatype, "date")

        c = DimensionColumn("foo", DataTypeser.test_datetime)
        self.assertEqual(c.datatype, "datetime")

    def test_metric_column_datatypes(self):
        c = MetricColumn("foo", func.sum(DataTypeser.score) * 2.0)
        self.assertEqual(c.datatype, "num")

        c = MetricColumn("foo", func.count(DataTypeser.department))
        self.assertEqual(c.datatype, "num")

        # Technically invalid but sqlalchemy doesn't recognize the issue
        c = MetricColumn("foo", func.sum(DataTypeser.department))
        self.assertEqual(c.datatype, "str")

    def test_metric_column_from_field(self):
        # Fields get automatically aggregated
        c = MetricColumn.from_field(DataTypeser, "foo", "score")
        self.assertEqual(c.datatype, "num")
        self.assertEqual(str(c), "sum(datatypes.score)")

        c = MetricColumn.from_field(DataTypeser, "foo", "department")
        self.assertEqual(c.datatype, "num")
        self.assertEqual(str(c), "count(datatypes.department)")

        c = MetricColumn.from_field(DataTypeser, "foo", "min(department)")
        # Incorrect datatype
        self.assertEqual(c.datatype, "num")
        self.assertEqual(str(c), "min(datatypes.department)")

        c = MetricColumn.from_field(DataTypeser, "foo", "sum(score)*2")
        self.assertEqual(c.datatype, "num")
        self.assertEqual(str(c), "sum(datatypes.score) * 2")

    def test_caching(self):
        """We cache the builder and the parse results """
        for i in range(1000):
            c = MetricColumn.from_field(DataTypeser, "foo", f"sum(score)*2.22")
            self.assertEqual(str(c), "sum(datatypes.score) * 2.22")

    def test_caching_lru_cache(self):
        """We cache the builder and the parse results """
        for i in range(1000):
            c = MetricColumn.from_field(DataTypeser, "foo", f"sum(score)*{i%10}")
            self.assertEqual(c.datatype, "num")
