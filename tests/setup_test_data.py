import os
from datetime import date

from dateutil.relativedelta import relativedelta
from sqlalchemy import Boolean, Column, Date, DateTime, Float, Integer, String, Table
from yaml import safe_load
from dotenv import load_dotenv
from recipe.dbinfo.dbinfo import get_dbinfo
from functools import partial
from recipe.dbinfo.pool import SimplePool

load_dotenv()

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))

sqlite_db = os.path.join(ROOT_DIR, "testdata.db")


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


class SetupData:
    """Setup databases for testing"""

    def __init__(self, connection_string: str, **kwargs):
        self.dbinfo = get_dbinfo(connection_string, **kwargs)
        self.engine = self.dbinfo.engine
        self.meta = self.dbinfo.sqlalchemy_meta

    def load_data(self, table_name: str, table):
        """Load data from the data/ directory"""

        def chunk_list(lst, chunk_size):
            for i in range(0, len(lst), chunk_size):
                yield lst[i : i + chunk_size]

        data = safe_load(open(os.path.join(ROOT_DIR, "data", f"{table_name}.yml")))
        try:
            with self.dbinfo.engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as con:
                for chunk in chunk_list(data, 50):
                    print(f"Loading chunk from {table_name}")
                    con.execute(table.insert(), chunk)
        except NotImplementedError:
            with self.dbinfo.engine.connect() as con:
                for chunk in chunk_list(data, 50):
                    print(f"Loading chunk from {table_name}")
                    con.execute(table.insert(), chunk)

    def setup(self):
        """Set up tables using a connection_string

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

        self.meta.drop_all(bind=self.engine)
        self.meta.create_all(bind=self.engine)

        data = [
            [
                "weird_table_with_column_named_true_table",
                weird_table_with_column_named_true_table,
            ],
            ["basic_table", basic_table],
            ["scores_table", scores_table],
            ["datatypes_table", datatypes_table],
            ["scores_with_nulls_table", scores_with_nulls_table],
            ["tagscores_table", tagscores_table],
            ["id_tests_table", id_tests_table],
            ["census_table", census_table],
            ["state_fact_table", state_fact_table],
        ]

        callables = [partial(self.load_data, name, tbl) for name, tbl in data]
        SimplePool(callables=callables, pool_max=10).get_data()

        # Load the datetester_table with dynamic date data
        start_dt = date(date.today().year, date.today().month, 1)
        data = [
            {"dt": start_dt + relativedelta(months=offset_month), "count": 1}
            for offset_month in range(-50, 50)
        ]
        try:
            with self.dbinfo.engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as con:
                con.execute(datetester_table.insert(), data)
        except NotImplementedError:
            with self.dbinfo.engine.connect() as con:
                con.execute(datetester_table.insert(), data)


if __name__ == "__main__":
    d = SetupData(f"sqlite:///{sqlite_db}", echo=True)
    d.setup()

    d = SetupData(
        f"postgresql+psycopg2://postgres:postgres@db:5432/postgres", echo=True
    )
    d.setup()

    print(get_bigquery_connection_string())

    d = SetupData(
        get_bigquery_connection_string(), echo=True, **get_bigquery_engine_kwargs()
    )
    # Google cloud setup takes a long time, so it's disabled by default
    # d.setup()
