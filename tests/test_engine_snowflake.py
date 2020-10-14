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
from recipe.extensions import (
    Anonymize,
    AutomaticFilters,
    BlendRecipe,
    CompareRecipe,
    RecipeExtension,
    SummarizeOver,
    Paginate,
)

SNOWFLAKE_CONN_STR = os.environ.get("SNOWFLAKE_CONN_STR", "")


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
        kwargs["extension_classes"] = [AutomaticFilters]
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
        engine=engine, schema=schema, session_factory=session, sqlalchemy_meta=metadata,
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


def test_snowflake_conn():
    dbinfo = get_dbinfo(SNOWFLAKE_CONN_STR, schema="TPCDS_SF100TCL")
    tables = list_tables(dbinfo=dbinfo)
    assert tables == {
        "TPCDS_SF100TCL": [
            "call_center",
            "catalog_page",
            "catalog_returns",
            "catalog_sales",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "dbgen_version",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_returns",
            "store_sales",
            "time_dim",
            "warehouse",
            "web_page",
            "web_returns",
            "web_sales",
            "web_site",
        ]
    }


class TestParsedSQLGeneration(object):
    """More tests of SQL generation on complex parsed expressions """

    def setup(self):
        self.dbinfo = get_dbinfo(SNOWFLAKE_CONN_STR, schema="TPCDS_SF100TCL")

    def create_shelf(self, config, dbinfo, tablename):
        table = get_table(dbinfo, tablename)
        return Shelf.from_validated_yaml(config, table)

    def test_count(self):
        config = """
_version: 2
count_star:
    kind: Metric
    field: "count(*)"
"""
        shelf = self.create_shelf(config, self.dbinfo, "HOUSEHOLD_DEMOGRAPHICS")
        recipe = self.dbinfo.recipe(shelf=shelf).metrics("count_star")
        print(recipe.to_sql())
        print(recipe.dataset.csv)
        assert (
            recipe.to_sql()
            == '''SELECT count(*) AS count_star
FROM "TPCDS_SF100TCL"."HOUSEHOLD_DEMOGRAPHICS"'''
        )
        assert recipe.dataset.csv.replace("\r\n", "\n") == "count_star\n7200\n"
