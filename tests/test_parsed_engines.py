# Test parsed config against different database engines
import os

import attr
from sqlalchemy import (
    MetaData,
    Table,
    create_engine,
)
from sqlalchemy.orm import sessionmaker
from recipe.shelf import Shelf
from recipe.core import Recipe


REDSHIFT_CONN_STR = os.environ.get("REDSHIFT_CONN_STR", "")
BIGQUERY_PRIVATE_KEY = os.environ.get("BIGQUERY_PRIVATE_KEY", "").replace("\\n", "\n")
BIGQUERY_CONN_STR = "bigquery://juicebox-open-test"


gcloud_creds = {
    "type": "service_account",
    "project_id": "juicebox-open-test",
    "private_key_id": "d68801427c72e296a47532bcc8dbe856a4116871",
    "private_key": BIGQUERY_PRIVATE_KEY,
    "client_email": "jbo-test-bigquery-admin@juicebox-open-test.iam.gserviceaccount.com",
    "client_id": "114757123849235966640",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/jbo-test-bigquery-admin%40juicebox-open-test.iam.gserviceaccount.com",
}


def list_tables(dbinfo):
    """Return a dict of {schema_name: [table]} for a connection."""
    tables = {}
    with dbinfo.engine.connect() as conn:
        if dbinfo.schema:
            tables[dbinfo.schema] = sorted(
                dbinfo.engine.table_names(schema=dbinfo.schema, connection=conn)
            )
        else:
            for schema in dbinfo.engine.dialect.get_schema_names(conn):
                tables[schema] = sorted(
                    dbinfo.engine.table_names(schema=schema, connection=conn)
                )
    return tables


def get_bigquery_table(schema, table):
    conn_string = "bigquery://{}/{}".format(settings.GOOGLE_CLOUD_PROJECT, schema)
    return get_sql_table(
        conn_string, table, engine_kwargs={"credentials_info": get_gcloud_credentials()}
    )


@attr.s
class DBInfo(object):
    """An object for keeping track of some SQLAlchemy objects related to a
    single database and optional schema.
    """

    engine = attr.ib()
    schema = attr.ib()
    session_factory = attr.ib()
    sqlalchemy_meta = attr.ib()

    def recipe(self, **kwargs):
        return Recipe(session=self.session_factory(), **kwargs)


def get_dbinfo(conn_string, schema=None, engine_kwargs=None):
    """Get a DBInfo object based on a connection string."""
    if engine_kwargs is None:
        engine_kwargs = {}
    if conn_string.startswith("bigquery") and schema:
        conn_string = "{}/{}".format(conn_string, schema)
        schema = None
    engine = create_engine(conn_string, **engine_kwargs)
    session = sessionmaker(engine)
    metadata = MetaData(bind=engine)
    return DBInfo(
        engine=engine,
        schema=schema,
        session_factory=session,
        sqlalchemy_meta=metadata,
    )


def get_sql_table(connection_string, table, engine_kwargs=None):
    schema = None
    if "." in table:
        schema, table = table.split(".")
    dbinfo = get_dbinfo(connection_string, schema=schema, engine_kwargs=engine_kwargs)
    table = load_table(dbinfo.engine, dbinfo.sqlalchemy_meta, table)
    return dbinfo, table


def get_table(dbinfo, table):
    if dbinfo.schema:
        return Table(
            table,
            dbinfo.sqlalchemy_meta,
            schema=dbinfo.schema,
            autoload=True,
            autoload_with=dbinfo.engine,
        )
    else:
        return Table(
            table, dbinfo.sqlalchemy_meta, autoload=True, autoload_with=dbinfo.engine
        )


# def test_redshift_conn():
#     dbinfo = get_dbinfo(REDSHIFT_CONN_STR, schema="recipetest")
#     tables = list_tables(dbinfo=dbinfo)
#     assert tables == {
#         "recipetest": [
#             "census",
#             "datetester",
#             "foo",
#             "scores",
#             "scores_with_nulls",
#             "state_fact",
#             "tagscores",
#         ]
#     }


# def test_bq_conn():
#     dbinfo = get_dbinfo(
#         BIGQUERY_CONN_STR,
#         schema="recipetest",
#         engine_kwargs={"credentials_info": gcloud_creds},
#     )
#     tables = list_tables(dbinfo=dbinfo)
#     assert tables == {
#         "recipetest": [
#             "census",
#             "datetester",
#             "foo",
#             "scores",
#             "scores_with_nulls",
#             "state_fact",
#             "tagscores",
#         ]
#     }


class TestParsedSQLGeneration(object):
    """More tests of SQL generation on complex parsed expressions """

    def setup(self):
        self.redshift_dbinfo = get_dbinfo(REDSHIFT_CONN_STR, schema="recipetest")
        self.bq_dbinfo = get_dbinfo(
            BIGQUERY_CONN_STR,
            schema="recipetest",
            engine_kwargs={"credentials_info": gcloud_creds},
        )

    def create_shelf(self, config, dbinfo, tablename):
        table = get_table(dbinfo, tablename)
        return Shelf.from_validated_yaml(config, table)

    def test_redshift(self):
        config = """
_version: 2
count_star:
    kind: Metric
    field: "count(*)"        
pctpop2000:
    kind: Metric
    field: "percentile5(pop2000)"        
"""
        shelf = self.create_shelf(config, self.redshift_dbinfo, "census")
        recipe = self.redshift_dbinfo.recipe(shelf=shelf).metrics(
            "count_star", "pctpop2000"
        )
        print(recipe.to_sql())
        assert (
            recipe.to_sql()
            == """SELECT count(*) AS count_star,
       percentile_cont(0.05) WITHIN GROUP (
                                           ORDER BY recipetest.census.pop2000) AS pctpop2000
FROM recipetest.census"""
        )
        assert (
            recipe.dataset.csv.replace("\r\n", "\n")
            == "count_star,pctpop2000\n344,1723.60\n"
        )

    def test_bigquery(self):
        # Percentile not supported on bq
        config = """
_version: 2
count_star:
    kind: Metric
    field: "count(*)"        
pctpop2000:
    kind: Metric
    field: "percentile5(pop2000)"        
"""
        shelf = self.create_shelf(config, self.bq_dbinfo, "census")
        recipe = self.bq_dbinfo.recipe(shelf=shelf).metrics("count_star", "pctpop2000")
        assert (
            recipe.dataset.csv.replace("\r\n", "\n")
            == "count_star,pctpop2000\n344,1723\n"
        )

    def test_redshift_datetester(self):
        config = """
_version: 2
yeardt:
    kind: Dimension
    field: year(dt)
count_star:
    kind: Metric
    field: "count(*)"        
"""
        shelf = self.create_shelf(config, self.redshift_dbinfo, "datetester")
        recipe = (
            self.redshift_dbinfo.recipe(shelf=shelf)
            .metrics("count_star")
            .dimensions("yeardt")
        )
        assert (
            recipe.to_sql()
            == """SELECT date_trunc('year', recipetest.datetester.dt) AS yeardt,
       count(*) AS count_star
FROM recipetest.datetester
GROUP BY yeardt"""
        )
        assert (
            recipe.dataset.csv.replace("\r\n", "\n")
            == """yeardt,count_star,yeardt_id
2017-01-01 00:00:00,12,2017-01-01 00:00:00
2018-01-01 00:00:00,12,2018-01-01 00:00:00
2019-01-01 00:00:00,12,2019-01-01 00:00:00
2020-01-01 00:00:00,12,2020-01-01 00:00:00
2021-01-01 00:00:00,12,2021-01-01 00:00:00
2022-01-01 00:00:00,12,2022-01-01 00:00:00
2023-01-01 00:00:00,12,2023-01-01 00:00:00
2024-01-01 00:00:00,12,2024-01-01 00:00:00
2025-01-01 00:00:00,12,2025-01-01 00:00:00
2026-01-01 00:00:00,12,2026-01-01 00:00:00
2027-01-01 00:00:00,12,2027-01-01 00:00:00
2028-01-01 00:00:00,12,2028-01-01 00:00:00
2029-01-01 00:00:00,12,2029-01-01 00:00:00
2030-01-01 00:00:00,12,2030-01-01 00:00:00
"""
        )

    def test_bigquery_datetester(self):
        config = """
_version: 2
yeardt:
    kind: Dimension
    field: year(dt)
cntorig:
    kind: Metric
    field: count_distinct(dt)
count_star:
    kind: Metric
    field: "count(*)"        
"""
        shelf = self.create_shelf(config, self.bq_dbinfo, "datetester")
        recipe = (
            self.bq_dbinfo.recipe(shelf=shelf)
            .metrics("count_star", "cntorig")
            .dimensions("yeardt")
        )
        assert (
            recipe.to_sql()
            == """SELECT date_trunc(`datetester`.`dt`, year) AS `yeardt`,
       count(DISTINCT `datetester`.`dt`) AS `cntorig`,
       count(*) AS `count_star`
FROM `datetester`
GROUP BY `yeardt`"""
        )
        print(recipe.dataset.csv)
        assert (
            recipe.dataset.csv.replace("\r\n", "\n")
            == """yeardt,cntorig,count_star,yeardt_id
2017-01-01,12,12,2017-01-01
2019-01-01,12,12,2019-01-01
2021-01-01,12,12,2021-01-01
2024-01-01,12,12,2024-01-01
2026-01-01,12,12,2026-01-01
2020-01-01,12,12,2020-01-01
2023-01-01,12,12,2023-01-01
2027-01-01,12,12,2027-01-01
2018-01-01,12,12,2018-01-01
2028-01-01,12,12,2028-01-01
2030-01-01,12,12,2030-01-01
2022-01-01,12,12,2022-01-01
2025-01-01,12,12,2025-01-01
2029-01-01,12,12,2029-01-01
"""
        )
