import logging
import time
import warnings
from copy import copy, deepcopy
from uuid import uuid4

from orderedset import OrderedSet
from sqlalchemy import (alias)
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.util import (lightweight_named_tuple)

from recipe.compat import *
from recipe.exceptions import BadRecipe
from recipe.ingredients import Dimension, Metric, Filter, Having
from recipe.shelf import Shelf
from recipe.utils import prettyprintable_sql

ALLOW_QUERY_CACHING = True

warnings.simplefilter('always', DeprecationWarning)

logger = logging.getLogger(__name__)


# TODO mixin approach
# Stats
# Anonymize (could be configured for local or cached)
# Automatic filters
# Query caching
# Expose a logger
#
# config object that you could use instead of passing data in
#  on every call, control settings via a yamlfile/env variable/object


class Stats(object):
    def __init__(self):
        self.ready = False

    def set_stats(self, rows, dbtime, enchanttime, from_cache):
        self.ready = True
        self._rows = rows
        self._dbtime = dbtime
        self._enchanttime = enchanttime
        self._from_cache = from_cache

    def _get_value(self, val):
        if self.ready:
            return val
        else:
            raise BadRecipe("Can't access stats before the query has run")

    @property
    def rows(self):
        """ The number of rows in this result. """
        return self._get_value(self._rows)

    @property
    def dbtime(self):
        """ The amount of time the database took to process. """
        return self._get_value(self._dbtime)

    @property
    def enchanttime(self):
        """ The amount of time the database took to process. """
        return self._get_value(self._enchanttime)

    @property
    def from_cache(self):
        """ Was this result cached """
        return self._get_value(self._from_cache)


class Recipe(object):
    """ Builds a query using Ingredients.
    """

    def __init__(self,
                 shelf=None,
                 metrics=None,
                 dimensions=None,
                 filters=None,
                 order_by=None,
                 automatic_filters=None,
                 session=None):
        """
        :param shelf: A shelf to use for looking up
        :param metrics:
        :param dimensions:
        :param filters:
        :param order_by:
        :param session:
        """

        self._id = uuid4()
        self._metrics = OrderedSet()
        self._dimensions = OrderedSet()
        self._filters = OrderedSet()
        self.order_bys = OrderedSet()
        self._order_bys = []

        self.shelf(shelf)

        if automatic_filters is None:
            self.automatic_filters = {}
        else:
            self.automatic_filters = automatic_filters
        self._apply_automatic_filters = True
        self.cache_context = None
        self.stats = Stats()

        if metrics is not None:
            self.metrics(*metrics)
        if dimensions is not None:
            self.dimensions(*dimensions)
        if filters is not None:
            self.filters(*filters)
        if order_by is not None:
            self.order_by(*order_bys)

        self._session = session

        self._limit = 0
        self._offset = 0

        self._is_postgres_engine = None
        # Store cached results in _query and _all
        # setting dirty to true invalidates these caches
        self.dirty = True
        # Have the rows been fetched
        self.all_dirty = True
        self._query = None
        self._all = []

        # Stores all ingredients used in the recipe
        self._cauldron = _Cauldron()

    # -------
    # Builder for parts of the recipe.
    # -------

    def shelf(self, shelf=None):
        """ Defines a shelf to use for this recipe """
        if shelf is None:
            self._shelf = Shelf({})
        elif isinstance(shelf, dict):
            self._shelf = Shelf(shelf)
        elif isinstance(shelf, Shelf):
            self._shelf = shelf
        else:
            raise BadRecipe("shelf must be a dict or recipe.shelf.Shelf")

    def metrics(self, *metrics):
        """ Add a list of Metric ingredients to the query. These can either be
        Metric objects or strings representing metrics on the shelf.

        The Metric expression will be added to the query's select statement.
        The metric value is a property of each row of the result.

        :param *metrics: Metrics to add to the recipe. Metrics can
                         either be keys on the ``shelf`` or
                         Metric objects
        :type *metrics: list
        """
        cleaned_metrics = []
        for m in metrics:
            cleaned_metrics.append(self._shelf.find(m, Metric))

        new_metrics = OrderedSet(cleaned_metrics)
        if new_metrics != self._metrics:
            self._metrics = new_metrics
            self.dirty = True
        return self

    @property
    def metric_ids(self):
        return (m.id for m in self._metrics)

    def dimensions(self, *dimensions):
        """ Add a list of Dimension ingredients to the query. These can either be
        Dimension objects or strings representing dimensions on the shelf.

        The Dimension expression will be added to the query's select statement
        and to the group_by.

        :param *dimensions: Dimensions to add to the recipe. Dimensions can
                         either be keys on the ``shelf`` or
                         Dimension objects
        :type *dimensions: list
        """
        cleaned_dimensions = []
        for d in dimensions:
            cleaned_dimensions.append(self._shelf.find(d, Dimension))

        new_dimensions = OrderedSet(cleaned_dimensions)
        if new_dimensions != self._dimensions:
            self._dimensions = new_dimensions
            self.dirty = True
        return self

    @property
    def dimension_ids(self):
        return (d.id for d in self._dimensions)

    def filters(self, *filters):
        """
        Add a list of Filter ingredients to the query. These can either be
        Filter objects or strings representing filters on the service's shelf.
        ``.filters()`` are additive, calling .filters() more than once will add
        to the list of filters being used by the recipe.

        The Filter expression will be added to the query's where clause

        :param *filters: Filters to add to the recipe. Filters can
                         either be keys on the ``shelf`` or
                         Filter objects
        :type *filters: list
        """

        def filter_constructor(f, shelf=None):
            if isinstance(f, BinaryExpression):
                return Filter(f)
            else:
                return f

        cleaned_filters = OrderedSet()
        for f in filters:
            cleaned_filters.add(self._shelf.find(f, (Filter, Having),
                                                 constructor=filter_constructor))

        new_filters = self._filters.union(cleaned_filters)
        if new_filters != self._filters:
            self._filters = new_filters
            self.dirty = True
        return self

    @property
    def filter_ids(self):
        return (f.id for f in self._filters)

    def order_by(self, *order_bys):
        """ Add a list of ingredients to order by to the query. These can
        either be Dimension or Metric objects or strings representing
        order_bys on the shelf.

        The Order_by expression will be added to the query's order_by statement

        :param *order_bys: Order_bys to add to the recipe. Order_bys can
                         either be keys on the ``shelf`` or
                         Dimension or Metric objects. If the
                         key is prefixed by "-" the ordering will be
                         descending.
        :type *order_bys: list
        """
        cleaned_order_bys = []
        for ingr in order_bys:
            # TODO: python3
            if isinstance(ingr, basestring):
                desc = False
                if ingr.startswith('-'):
                    desc = True
                    ingr = ingr[1:]
                if ingr not in self._shelf:
                    raise BadRecipe("{} doesn't exist on the shelf".format(
                        ingr))
                ingr = self._shelf[ingr]
                if not isinstance(ingr, (Dimension, Metric)):
                    raise BadRecipe(
                        "{} is not a Dimension or Metric".format(ingr))
                if desc:
                    # Make a copy to ensure we don't have any side effects
                    # then set the ordering property on the ingredient
                    ingr = deepcopy(ingr)
                    ingr.ordering = 'desc'

                cleaned_order_bys.append(ingr)
            if isinstance(ingr, (Dimension, Metric)):
                cleaned_order_bys.append(ingr)
            else:
                raise BadRecipe("{} is not a order_by".format(ingr))

        new_order_bys = OrderedSet(cleaned_order_bys)
        if new_order_bys != self._order_bys:
            self._order_bys = new_order_bys
            self.dirty = True
        return self

    def session(self, session):
        self.dirty = True
        self._session = session
        return self

    def limit(self, limit):
        """ Limit the number of rows returned from the database.

        :param limit: The number of rows to return in the recipe. 0 will return
                      all rows.
        :type limit: int
        """
        if self._limit != limit:
            self.dirty = True
            self._limit = limit
        return self

    # ------
    # Utility functions
    # ------

    def _gather_all_ingredients_into_cauldron(self):
        self._cauldron.empty()

        ingredients = self._metrics.union(self._dimensions).union(self._filters)
        for ingredient in ingredients:
            self._cauldron.use(ingredient)

    def _is_postgres(self):
        """ Determine if the running engine is postgres """
        if self._is_postgres_engine is None:
            is_postgres_engine = False
            try:
                dialect = self.session.bind.engine.name
                if 'redshift' in dialect or 'postg' in dialect or 'pg' in \
                    dialect:
                    is_postgres_engine = True
            except:
                pass
            self._is_postgres_engine = is_postgres_engine
        return self._is_postgres_engine

    def _prepare_order_bys(self):
        """ Build a set of order by columns """
        order_bys = OrderedSet()
        if self._order_bys:
            for ingredient in self._order_bys:
                if isinstance(ingredient, Dimension):
                    # Reverse the ordering columns so that dimensions
                    # order by their label rather than their id
                    columns = reversed(ingredient.columns)
                else:
                    columns = ingredient.columns
                for c in columns:
                    order_by = c.desc() if ingredient.ordering == 'desc' else c
                    if unicode(order_by) not in [unicode(o) for o in order_bys]:
                        order_bys.add(order_by)

        return list(order_bys)

    def query(self):
        """ Generates a query using the Dimension, Measure, and Filter
        ingredients supplied by the recipe.
        """
        if not self.dirty and self._query:
            return self._query

        # Step 1: Gather up global filters and user filters and
        # apply them as if they had been added to recipe().filters(...)

        order_bys = self._prepare_order_bys()

        # Step 2: Build the query (now that it has all the filters
        # and apply any blend recipes

        # Gather the ingredients and add them to the cauldron
        self._gather_all_ingredients_into_cauldron()

        # Get the parts of the query from the cauldron
        # We don't need to regather order_bys
        columns, group_bys, filters, havings = self._cauldron.brew_query_parts()

        # Start building the query
        query = self._session.query(*columns)
        # TODO: .options(FromCache("default"))

        # Only add group_bys at this point because using blend queries
        # may have added more group_bys
        query = query.group_by(*group_bys).order_by(*order_bys).filter(
            *filters)
        if havings:
            for having in havings:
                query = query.having(having)

        if len(query.selectable.froms) != 1:
            raise BadRecipe("Recipes must use ingredients that all come from "
                            "the same table. \nDetails on this recipe:\n{"
                            "}".format(str(self._cauldron)))

        # Apply limit on the outermost query
        # This happens after building the comparison recipe
        if self._limit and self._limit > 0:
            query = query.limit(self._limit)

        # Step 5:  Clear the dirty flag,
        # Patch the query if there's a comparison query
        # cache results

        self._query = query
        self.dirty = False
        return self._query

    def table(self):
        """ A convenience method to determine the table the query is selecting from
        """
        query_table = self.query().selectable.froms[0]
        if self._table:
            if self._table == query_table:
                return self._table
            else:
                raise BadRecipe('Recipe was passed a table which is not the '
                                'table it is selecting from')
        else:
            return query_table

    def to_sql(self):
        """ A string representation of the SQL this recipe will generate.
        """
        return prettyprintable_sql(self.query())

    def subquery(self, name=None):
        """ The recipe's query as a subquery suitable for use in joins or other
        queries.
        """
        query = self.query()
        return query.subquery(name=name)

    def as_table(self):
        """ Return an alias to a table
        """
        return alias(self.subquery(), name=self.id)

    def all(self):
        """ Return a (potentially cached) list of result objects.
        """
        starttime = fetchtime = enchanttime = time.time()
        fetched_from_cache = False

        if self.dirty or self.all_dirty:
            query = self.query()
            self._all = query.all()
            # If we're using a caching query and that query did not
            # save new values to cache, we got the cached results
            # This is not 100% accurate; it only reports if the caching query
            # attempts to save to cache not the internal state of the cache
            # and whether the cache save actually occurred.
            if not getattr(query, 'saved_to_cache', True):
                fetched_from_cache = True
            fetchtime = time.time()
            self._all = self._cauldron.enchant(
                self._all,
                cache_context=self.cache_context)
            enchanttime = time.time()

            self.all_dirty = False
        else:
            # In this case we are using the object self._all as cache
            fetched_from_cache = True

        self.stats.set_stats(len(self._all),
                             fetchtime - starttime,
                             enchanttime - fetchtime,
                             fetched_from_cache)

        return self._all

    def one(self):
        """ Return the first element on the result
        """
        all = self.all()
        if len(all) > 0:
            return all[0]
        else:
            return []

    def first(self):
        """ Return the first element on the result
        """
        return self.one()
