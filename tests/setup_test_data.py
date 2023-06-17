import os
from datetime import date

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
    create_engine,
)
from yaml import safe_load
from dotenv import load_dotenv
from .test_base import get_bigquery_engine_kwargs, get_bigquery_connection_string

load_dotenv()

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))

sqlite_db = os.path.join(ROOT_DIR, "testdata.db")


class SetupData:
    """Setup databases for testing"""

    def __init__(self, connection_string, **kwargs):
        self.engine = create_engine(connection_string, **kwargs)
        self.meta = MetaData(bind=self.engine)

    def load_data(self, table_name, table):
        """Load data from the data/ directory"""
        data = safe_load(open(os.path.join(ROOT_DIR, "data", f"{table_name}.yml")))
        self.engine.execute(table.insert(), data)

    def setup(self):
        """Set up tables using a connection_string to define an oven.

        Tables are loaded using data in data/{tablename}.yml
        """
        self.root_dir = os.path.abspath(os.path.dirname(__file__))

        weird_table_with_column_named_true_table = Table(
            "weird_table_with_column_named_true", self.meta, Column("true", String)
        )

        basic_table = Table(
            "foo",
            self.meta,
            Column("first", String),
            Column("last", String),
            Column("age", Integer),
            Column("birth_date", Date),
            Column("dt", DateTime),
        )

        datetester_table = Table(
            "datetester", self.meta, Column("dt", Date), Column("count", Integer)
        )

        scores_table = Table(
            "scores",
            self.meta,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
        )

        datatypes_table = Table(
            "datatypes",
            self.meta,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
            Column("test_datetime", DateTime),
            Column("valid_score", Boolean),
        )

        scores_with_nulls_table = Table(
            "scores_with_nulls",
            self.meta,
            Column("username", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
            Column("test_date", Date),
        )

        tagscores_table = Table(
            "tagscores",
            self.meta,
            Column("username", String),
            Column("tag", String),
            Column("department", String),
            Column("testid", String),
            Column("score", Float),
        )

        id_tests_table = Table(
            "id_tests",
            self.meta,
            Column("student", String),
            Column("student_id", Integer),
            Column("age", Integer),
            Column("age_id", Integer),
            Column("score", Float),
        )

        census_table = Table(
            "census",
            self.meta,
            Column("state", String),
            Column("sex", String),
            Column("age", Integer),
            Column("pop2000", Integer),
            Column("pop2008", Integer),
        )

        state_fact_table = Table(
            "state_fact",
            self.meta,
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
        )

        self.meta.drop_all()
        self.meta.create_all()
        # self.engine.drop_all()
        # self.engine.create_all()
        self.load_data(
            "weird_table_with_column_named_true_table",
            weird_table_with_column_named_true_table,
        )
        self.load_data("basic_table", basic_table)
        self.load_data("scores_table", scores_table)
        self.load_data("datatypes_table", datatypes_table)
        self.load_data("scores_with_nulls_table", scores_with_nulls_table)
        self.load_data("tagscores_table", tagscores_table)
        self.load_data("id_tests_table", id_tests_table)
        self.load_data("census_table", census_table)
        self.load_data("state_fact_table", state_fact_table)

        # Load the datetester_table with dynamic date data
        start_dt = date(date.today().year, date.today().month, 1)
        data = [
            {"dt": start_dt + relativedelta(months=offset_month), "count": 1}
            for offset_month in range(-50, 50)
        ]
        self.engine.execute(datetester_table.insert(), data)


if __name__ == "__main__":
    d = SetupData(f"sqlite:///{sqlite_db}", echo=True)
    d.setup()

    d = SetupData(
        f"postgresql+psycopg2://postgres:postgres@db:5432/postgres", echo=True
    )
    d.setup()

    d = SetupData(
        get_bigquery_connection_string(), echo=True, **get_bigquery_engine_kwargs()
    )
    # Google cloud setup takes a long time, so it's disabled by default
    # d.setup()
