""" mappers.py
Introduces a query option called FromCache.

The three new concepts introduced here are:

 * FromCache - a query option that establishes caching
   parameters on a Query
 * RelationshipCache - a variant of FromCache which is specific
   to a query invoked during a lazy load.
 * _params_from_query - extracts value parameters from
   a Query.
"""
from sqlalchemy.orm.interfaces import MapperOption


class FromCache(MapperOption):
    """Specifies that a Query should load results from a cache."""

    propagate_to_loaders = False

    def __init__(self, region="default", cache_key=None, cache_prefix=None):
        """Construct a new FromCache.
        :param region: the cache region.  Should be a
        region configured in the dictionary of dogpile
        regions.
        :param cache_key: optional.  A string cache key
        that will serve as the key to the query.   Use this
        if your query has a huge amount of parameters (such
        as when using in_()) which correspond more simply to
        some other identifier.
        """
        self.region = region
        self.cache_key = cache_key
        self.cache_prefix = cache_prefix

    def process_query(self, query):
        """Process a Query during normal loading operation."""
        query._cache_region = self


class RelationshipCache(MapperOption):
    """Specifies that a Query as called within a "lazy load"
    should load results from a cache."""

    propagate_to_loaders = True

    def __init__(self, attribute, region="default", cache_key=None):
        """Construct a new RelationshipCache.

        :param attribute: A Class.attribute which
        indicates a particular class relationship() whose
        lazy loader should be pulled from the cache.

        :param region: name of the cache region.

        :param cache_key: optional.  A string cache key
        that will serve as the key to the query, bypassing
        the usual means of forming a key from the Query itself.

        """
        self.region = region
        self.cache_key = cache_key
        self._relationship_options = {
            (attribute.property.parent.class_, attribute.property.key): self
        }

    def process_query_conditionally(self, query):
        """Process a Query that is used within a lazy loader.

        (the process_query_conditionally() method is a SQLAlchemy
        hook invoked only within lazyload.)

        """
        if query._current_path:
            mapper, prop = query._current_path[-2:]
            key = prop.key

            for cls in mapper.class_.__mro__:
                if (cls, key) in self._relationship_options:
                    relationship_option = self._relationship_options[(cls, key)]
                    query._cache_region = relationship_option
                    break

    def and_(self, option):
        """Chain another RelationshipCache option to this one.

        While many RelationshipCache objects can be specified on a single
        Query separately, chaining them together allows for a more efficient
        lookup during load.

        """
        self._relationship_options.update(option._relationship_options)
        return self
