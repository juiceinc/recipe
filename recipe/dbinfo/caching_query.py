"""caching_query.py

Represent functions and classes which allow the usage of
Dogpile caching with SQLAlchemy.

 * CachingQuery - a Query subclass that caches and
   retrieves results in/from dogpile.cache.

The rest of what's here are standard SQLAlchemy and dogpile.cache constructs.
"""
from dogpile.cache.api import NO_VALUE
from dogpile.util.readwrite_lock import LockError as DogpileLockError
from recipe.utils import clean_unicode, prettyprintable_sql
from redis.exceptions import ConnectionError, LockError as RedisLockError
from sqlalchemy.orm.query import Query
import structlog

SLOG = structlog.get_logger(__name__)


class CachingQuery(Query):
    """A Query subclass which optionally loads full results from a dogpile
    cache region.

    The CachingQuery optionally stores additional state that allows it to
    consult a dogpile.cache cache before accessing the database, in the form
    of a FromCache or RelationshipCache object.   Each of these objects
    refer to the name of a :class:`dogpile.cache.Region` that's been
    configured
    and stored in a lookup dictionary.  When such an object has associated
    itself with the CachingQuery, the corresponding dogpile.cache.Region
    is used to locate a cached result.  If none is present, then the
    Query is invoked normally, the results being cached.

    The FromCache and RelationshipCache mapper options below represent
    the "public" method of configuring this state upon the CachingQuery.
    """

    def __init__(self, regions, *args, **kw):
        self.cache_regions = regions
        self.fetched_from_cache = False
        self.format_log_sql = kw.pop("format_log_sql", True)
        Query.__init__(self, *args, **kw)

    def __iter__(self):
        """override __iter__ to pull results from dogpile
        if particular attributes have been configured.

        Note that this approach does *not* detach the loaded objects from
        the current session. If the cache backend is an in-process cache
        (like "memory") and lives beyond the scope of the current
        session's
        transaction, those objects may be expired. The method here can be
        modified to first expunge() each loaded item from the current
        session before returning the list of items, so that the items
        in the cache are not the same ones in the current Session.
        """
        if hasattr(self, "_cache_region"):
            return self.get_value(createfunc=lambda: list(self._log_and_query()))
        else:
            return self._log_and_query()

    def _log_and_query(self):
        SLOG.info(
            "execute-query", sql=prettyprintable_sql(self, reindent=self.format_log_sql)
        )
        return Query.__iter__(self)

    def _get_cache_plus_key(self):
        """Return a cache region plus key."""
        dogpile_region = self.cache_regions[self._cache_region.region]
        if self._cache_region.cache_key:
            key = self._cache_region.cache_key
        else:
            key = _key_from_query(self)
        key = "{}:{}".format(self._cache_region.cache_prefix, key)
        return dogpile_region, key

    def invalidate(self):
        """Invalidate the cache value represented by this Query."""
        dogpile_region, cache_key = self._get_cache_plus_key()
        dogpile_region.delete(cache_key)

    def get_value(
        self, merge=True, createfunc=None, expiration_time=None, ignore_expiration=False
    ):
        """Return the value from the cache for this query.

        Raise KeyError if no value present and no
        createfunc specified.
        """
        dogpile_region, cache_key = self._get_cache_plus_key()

        # ignore_expiration means, if the value is in the cache
        # but is expired, return it anyway.   This doesn't make sense
        # with createfunc, which says, if the value is expired, generate
        # a new value.
        assert (
            not ignore_expiration or not createfunc
        ), "Can't ignore expiration and also provide createfunc"

        ran_createfunc = []
        if ignore_expiration or not createfunc:
            cached_value = dogpile_region.get(
                cache_key,
                expiration_time=expiration_time,
                ignore_expiration=ignore_expiration,
            )
        else:

            def create_wrapper():
                # When we switch all the way to Python 3, this should use `nonlocal`
                ran_createfunc.append(True)
                return createfunc()

            try:
                cached_value = dogpile_region.get_or_create(
                    cache_key, create_wrapper, expiration_time=expiration_time
                )
            except ConnectionError:
                SLOG.error("Cannot connect to query caching backend!")
                cached_value = create_wrapper()
            except (RedisLockError, DogpileLockError) as exc_info:
                SLOG.warn("Lock is not longer available", exc_info=exc_info)
                cached_value = create_wrapper()
        if cached_value is NO_VALUE:
            raise KeyError(cache_key)
        if merge:
            cached_value = self.merge_result(cached_value, load=False)
        self.fetched_from_cache = not ran_createfunc
        return cached_value

    def set_value(self, value):
        """Set the value in the cache for this query."""
        dogpile_region, cache_key = self._get_cache_plus_key()
        try:
            dogpile_region.set(cache_key, value)
        except ConnectionError:
            SLOG.error("Cannot connect to query caching backend!")


def _key_from_query(query, qualifier=None):
    """Given a Query, create a cache key.

    There are many approaches to this; here we use the simplest,
    which is to create an md5 hash of the text of the SQL statement,
    combined with stringified versions of all the bound parameters
    within it.  There's a bit of a performance hit with
    compiling out "query.statement" here; other approaches include
    setting up an explicit cache key with a particular Query,
    then combining that with the bound parameter values.
    """
    stmt = query.with_labels().statement
    compiled = stmt.compile()
    params = compiled.params

    # here we return the key as a long string.  our "key mangler"
    # set up with the region will boil it down to an md5.
    return " ".join(
        [clean_unicode(compiled).decode("utf-8")]
        + [clean_unicode(params[k]).decode("utf-8") for k in sorted(params)]
    )
