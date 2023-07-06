import functools
import os
from contextlib import contextmanager
from threading import Lock

import attr
import cachetools
import structlog
from cachetools import TTLCache, cached
from sqlalchemy.engine import Engine
from sqlalchemy.sql.expression import Select
from sqlalchemy import MetaData, create_engine, event, exc
from sqlalchemy.orm import sessionmaker


from .caching_query import CachingQuery

SLOG = structlog.get_logger(__name__)


def query_callable(regions, query_cls=CachingQuery, **kwargs):
    return functools.partial(query_cls, regions, **kwargs)


def engine_is_postgres(engine):
    """Test if an engine is postgres compatible."""
    is_postgres = False
    pg_identifiers = ["redshift", "postg", "pg"]
    if any(pg_id in engine.name for pg_id in pg_identifiers):
        is_postgres = True
    return is_postgres


def connect(dbapi_connection, connection_record):
    connection_record.info["pid"] = os.getpid()


def checkout(dbapi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info["pid"] != pid:
        connection_record.dbapi_connection = connection_proxy.dbapi_connection = None
        raise exc.DisconnectionError(
            "Connection record belongs to pid %s, "
            "attempting to check out in pid %s" % (connection_record.info["pid"], pid)
        )


def refreshing_cached(cache, key=cachetools.keys.hashkey, lock=None):
    """Same as `cachetools.cached`,
    but it also refreshes the TTL and checks for expiry on read operations.
    """

    def decorator(func):
        func = cached(cache, key, lock)(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if lock is not None:
                with lock:
                    cache[key(*args, **kwargs)] = result
                    # cachetools also only auto-expires on *mutating*
                    # operations, so we need to be explicit:
                    cache.expire()
            return result

        return wrapper

    return decorator


def run_in_process(select: Select, engine: Engine):
    with engine.connect() as conn:
        return conn.execute(select).fetchall()


@attr.s
class DBInfo:
    """An object for keeping track of some SQLAlchemy objects related to a
    single database.
    """

    engine: Engine = attr.ib()
    session_factory: sessionmaker = attr.ib()
    sqlalchemy_meta: MetaData = attr.ib()
    metadata_write_lock: Lock = attr.ib()
    is_postgres: bool = attr.ib(default=False)

    def __attrs_post_init__(self):
        self.session = sessionmaker(self.engine)
        self.is_postgres = engine_is_postgres(self.engine)

    @property
    def Session(self):
        return self.session

    @property
    def drivername(self):
        return self.engine.url.drivername

    def execute(self):
        pass

    @contextmanager
    def connection_scope(self):
        """A Context Manager that manages a transaction around a block."""
        yield self.engine.connect()


# Decorate with an engine identifier
def make_engine_event_handler(event_name, engine_name):
    def event_handler(*args, event_name=event_name, engine_name=engine_name):
        log = SLOG.bind(engine_name=engine_name)
        log.info(event_name)

    return event_handler


def init_caching_session(engine=None):
    """Establishes a Session constructor for async database communications
    with caching queries.
    """
    if not engine:
        return

    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        query_cls=query_callable(regions, format_log_sql=format_log_sql),
    )


def init_session(engine=None):
    if not engine:
        return

    return sessionmaker(bind=engine)


#: A global cache of DBInfo, keyed by the connection string.
#: Cached for 10 minutes, but accesses will refresh the TTL.
_DBINFO_CACHE = TTLCache(maxsize=1024, ttl=600)
_DBINFO_CACHE_LOCK = Lock()


@refreshing_cached(
    cache=_DBINFO_CACHE,
    key=lambda conn_string, *a, **kw: conn_string + str(a) + str(kw),
    lock=_DBINFO_CACHE_LOCK,
)
def get_dbinfo(
    conn_string: str, use_caching: bool = False, debug: bool = False, **engine_kwargs
):
    """Get a (potentially cached) DBInfo object based on a connection string.

    Args:
        conn_string (str): A connection string
        use_caching (bool): Should caching be used

    Returns:
        DBInfo: A cached db info object
    """
    log = SLOG.bind(conn_string=conn_string, engine_kwargs=engine_kwargs)
    engine = create_engine(conn_string, **engine_kwargs)

    # Listen to events
    if debug:
        for event_name in (
            "checkout",
            "checkin",
            "close",
            "close_detached",
            "first_connect",
            "detach",
            "invalidate",
            "reset",
            "soft_invalidate",
            "engine_connect",
        ):
            event.listen(
                engine,
                event_name,
                make_engine_event_handler(
                    event_name=event_name, engine_name=engine.url
                ),
            )

    event.listen(engine, "connect", connect)
    event.listen(engine, "checkout", checkout)

    is_postgres = engine_is_postgres(engine)
    if use_caching:
        session = init_caching_session(engine)
    else:
        session = init_session(engine)
    sqlalchemy_meta = MetaData()

    dbinfo = DBInfo(
        engine=engine,
        session_factory=session,
        sqlalchemy_meta=sqlalchemy_meta,
        metadata_write_lock=Lock(),
        is_postgres=is_postgres,
    )
    return dbinfo
