# Connection utils

import threading
from datetime import date

import psycopg2
from gevent.socket import wait_read, wait_write
from psycopg2 import extensions
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from recipe import caching_query

POOL_SIZE = 5
POOL_RECYCLE = 60 * 60


def ping_connection(connection, branch):
    """
    SQLAlchemy code to pessimistically check if a connection has been
    disconnected
    See https://juiceanalytics.atlassian.net/browse/JB-606 for more details

    http://docs.sqlalchemy.org/en/latest/core/pooling.html#disconnect-handling-pessimistic

    :param connection:
    :param branch:
    :return:
    """
    if branch:
        # "branch" refers to a sub-connection of a connection,
        # we don't want to bother pinging on these.
        return

    try:
        # run a SELECT 1.   use a core select() so that
        # the SELECT of a scalar value without a table is
        # appropriately formatted for the backend
        connection.scalar(select([1]))
    except exc.DBAPIError as err:
        # catch SQLAlchemy's DBAPIError, which is a wrapper
        # for the DBAPI's exception.  It includes a .connection_invalidated
        # attribute which specifies if this connection is a "disconnect"
        # condition, which is based on inspection of the original exception
        # by the dialect in use.
        if err.connection_invalidated:
            # run the same SELECT again - the connection will re-validate
            # itself and establish a new connection.  The disconnect detection
            # here also causes the whole connection pool to be invalidated
            # so that all stale connections are discarded.
            connection.scalar(select([1]))
        else:
            raise
    except psycopg2.OperationalError as err:
        # Invalidate the underlying DBAPI connection associated with this
        # Connection
        connection.invalidate()

        # run the same SELECT again - the connection will re-validate
        # itself and establish a new connection.  The disconnect detection
        # here also causes the whole connection pool to be invalidated
        # so that all stale connections are discarded.
        connection.scalar(select([1]))


def init_engine(**kwargs):
    connection_str = kwargs.pop('connection_str', None)
    if not connection_str or connection_str is None:
        return

    connection_settings = {
        'pool_size': POOL_SIZE,
        'pool_recycle': POOL_RECYCLE
    }
    connection_settings.update(kwargs)

    engine = create_engine(connection_str, **connection_settings)

    # Add a handler to pessimistically check each time a connection is checked out
    event.listen(engine, "engine_connect", ping_connection)

    return engine


def init_engine_green(**kwargs):
    make_psycopg_green()
    connection_str = kwargs.pop('connection_str', None)
    if not connection_str or connection_str is None:
        return

    connection_settings = {
        'pool_size': POOL_SIZE,
        'pool_recycle': POOL_RECYCLE,
        'max_overflow': -1
    }

    connection_settings.update(kwargs)

    engine = create_engine(connection_str, **connection_settings)

    return engine


def init_session(engine=None):
    if not engine:
        return

    return sessionmaker(bind=engine)


def async_creation_runner(cache, somekey, creator, mutex):
    """ Used by dogpile.core:Lock when appropriate  """

    def runner():
        try:
            value = creator()
            cache.set(somekey, value)
        finally:
            mutex.release()

    thread = threading.Thread(target=runner)
    thread.start()


#
# redis_host, redis_port = settings.CACHES['default']['LOCATION'].split(':')
# regions = {}
#
# regions['default'] = make_region(async_creation_runner=async_creation_runner,
#                                  key_mangler=lambda
#                                  key: "fruition:dogpile:" + sha1_mangle_key(
#                                  key)).configure(
#     'dogpile.cache.redis',
#     arguments={
#         'host': redis_host,
#         'port': redis_port,
#         'db': settings.CACHES['default']['OPTIONS']['DB'],
#         'redis_expiration_time': 60 * 60 * 2,  # 2 hours
#         'distributed_lock': True,
#         'lock_timeout': 90,
#         'lock_sleep': 5
#     }
# )
#
# regions['anonymizer'] = make_region(
# async_creation_runner=async_creation_runner,
#                                     key_mangler=lambda
#                                         key: "anonymizer:" + sha1_mangle_key(
#                                         key)).configure(
#     'dogpile.cache.redis',
#     arguments={
#         'host': redis_host,
#         'port': redis_port,
#         'db': settings.CACHES['default']['OPTIONS']['DB'],
#         'redis_expiration_time': 60 * 60 * 2,  # 2 hours
#         'distributed_lock': True,
#         'lock_timeout': 90,
#         'lock_sleep': 5
#     }
# )


# Options for debugging queries.
# ------------

def format_params(params, statement):
    """
    Take a SQL statement with parameters and perform substitution
    so that it will make a clean statement when params are substituted
    into the string using python's % format operator

    :param params: Parameters that need to be cleaned
    :param statement: A statement that needs to be cleaned
    :return: A tuple of cleaned params and statement
    """
    if type(params) == type(()):
        # Do positional replacement of ? in statement
        new_statement = statement
        new_params = []
        for p in params:
            if isinstance(p, int):
                new_statement = new_statement.replace('?', '%d', 1)
                new_params.append(p)
            elif isinstance(p, int):
                new_statement = new_statement.replace('?', "'%s'", 1)
                new_params.append(p)
            else:
                new_statement = new_statement.replace('?', "%s", 1)
                new_params.append(str(p))
        return tuple(new_params), new_statement
    else:
        # Do name replacement
        new_params = {}
        for k, v in params.iteritems():
            if isinstance(v, basestring) and len(v) > 0 and v[0] != "'" and v[
                -1] != "'":
                new_params[k] = "'" + v + "'"
            elif isinstance(v, date):
                # Encode dates as strings
                new_params[k] = "'" + str(v) + "'"
            else:
                new_params[k] = v
        return new_params, statement


def make_psycopg_green():
    """Configure Psycopg to be used with gevent in non-blocking way.
    """
    if not hasattr(extensions, 'set_wait_callback'):
        raise ImportError(
            'support for coroutines not available in this Psycopg version '
            '(%s)' % psycopg2.__version__)

    extensions.set_wait_callback(gevent_wait_callback)


def gevent_wait_callback(conn, timeout=None):
    """A wait callback useful to allow gevent to work with Psycopg.
    """
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise psycopg2.OperationalError(
                "Bad result from poll: %r" % state)


def init_caching_session(engine=None):
    """ Establishes a Session constructor for async database communications
    with caching queries.
    """
    if not engine:
        return

    return sessionmaker(
        bind=engine, autoflush=False, autocommit=False,
        query_cls=caching_query.query_callable(regions)
    )
