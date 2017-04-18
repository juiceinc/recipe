import logging
import time
import warnings
from uuid import uuid4

import six
import tablib
from orderedset import OrderedSet
from sqlalchemy import (alias)
from sqlalchemy.sql.elements import BinaryExpression

from recipe.compat import *
from recipe.exceptions import BadRecipe
from recipe.ingredients import Dimension, Metric, Filter, Having
from recipe.shelf import Shelf
from recipe.utils import prettyprintable_sql

ALLOW_QUERY_CACHING = True

warnings.simplefilter('always', DeprecationWarning)

logger = logging.getLogger(__name__)



__title__ = 'recipe'
__version__ = '0.1.0'
__author__ = 'Chris Gemignani'
__license__ = 'MIT'
__copyright__ = 'Copyright 2017 Chris Gemignani'
__docformat__ = 'restructuredtext'


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

    def _get_value(self, prop):
        if self.ready:
            return getattr(self, prop)
        else:
            raise BadRecipe("Can't access stats before the query has run")

    @property
    def rows(self):
        """ The number of rows in this result. """
        return self._get_value('_rows')

    @property
    def dbtime(self):
        """ The amount of time the database took to process. """
        return self._get_value('_dbtime')

    @property
    def enchanttime(self):
        """ The amount of time the database took to process. """
        return self._get_value('_enchanttime')

    @property
    def from_cache(self):
        """ Was this result cached """
        return self._get_value('_from_cache')


class RecipeBase(type):
    def __new__(cls, name, bases, attrs):
        super_new = super(RecipeBase, cls).__new__

        # Also ensure initialization is only performed for subclasses of Model
        # (excluding Model class itself).
        parents = [b for b in bases if isinstance(b, RecipeBase)]
        if not parents:
            return super_new(cls, name, bases, attrs)

        # Create the class.
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        attr_meta = attrs.pop('Meta', None)
        return new_class

class Recipe(six.with_metaclass(RecipeBase)):
    """ Builds a query using Ingredients.

    recipe generates a query in the following way

        (RECIPE) recipe checks its dirty state and all extension dirty states to
        determine if the cached query needs to be regenerated

        (EXTENSIONS) all extension ``add_ingredients`` run to inject
        ingredients directly on the recipe

        (RECIPE) recipe runs gather_all_ingredients_into_cauldron to build a
        global lookup for ingredients

        (RECIPE) recipe runs cauldron.brew_query_parts to gather sqlalchemy
        columns, group_bys and filters

        (EXTENSIONS) all extension ``modify_sqlalchemy(columns,
        group_bys, filters)`` run to directly modify the collected
        sqlalchemy columns, group_bys or filters

        (RECIPE) recipe builds a preliminary query with columns

        (EXTENSIONS) all extension ``modify_sqlalchemy_prequery(query,
        columns, group_bys, filters)`` run to modify the query

        (RECIPE) recipe builds a full query with group_bys, order_bys,
        and filters.

        (RECIPE) recipe tests that this query only uses a single from

        (EXTENSIONS) all extension ``modify_sqlalchemy_postquery(query,
        columns, group_bys, order_bys filters)`` run to modify the query

        (RECIPE) recipe applies limits and offsets on the query

        (RECIPE) recipe caches completed query and sets all dirty flags to
        False.


    """

    def __init__(self,
                 shelf=None,
                 metrics=None,
                 dimensions=None,
                 filters=None,
                 order_by=None,
                 session=None,
                 extension_classes=None):
        """
        :param shelf: A shelf to use for looking up
        :param metrics:
        :param dimensions:
        :param filters:
        :param order_by:
        :param session:
        """

        self._id = str(uuid4())[:8]
        self.shelf(shelf)

        # Stores all ingredients used in the recipe
        self._cauldron = Shelf()
        self._order_bys = []

        self.cache_context = None
        self.stats = Stats()

        if metrics is not None:
            self.metrics(*metrics)
        if dimensions is not None:
            self.dimensions(*dimensions)
        if filters is not None:
            self.filters(*filters)
        if order_by is not None:
            self.order_by(*order_by)

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

        self.recipe_extensions = []
        if extension_classes is None:
            extension_classes = []

        for ExtensionClass in extension_classes:
            # Create all the extension instances, passing them a reference to
            # this recipe
            self.recipe_extensions.append(ExtensionClass(self))

        self._register_formats()

    # @classmethod
    def _register_formats(cls):
        """Adds format properties."""
        extensions = getattr(cls, 'extensions')


    # -------
    # Builder for parts of the recipe.
    # -------

    def __getattr__(self, name):
        """
        Return an attribute of self, if not found, proxy to all
        recipe_extensions

        :param name:
        :return:
        """
        try:
            return self.__getattribute__(name)
        except AttributeError:
            pass

        for extension in self.recipe_extensions:
            try:
                proxy_callable = getattr(extension, name)
                break
            except AttributeError:
                pass

        try:
            proxy_callable
        except NameError:
            raise AttributeError

        return proxy_callable

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
        return self

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
        for m in metrics:
            self._cauldron.use(self._shelf.find(m, Metric))
        self.dirty = True
        return self

    @property
    def metric_ids(self):
        return (m.id for m in self._cauldron.values() if isinstance(m, Metric))

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
        for d in dimensions:
            self._cauldron.use(self._shelf.find(d, Dimension))

        self.dirty = True
        return self

    @property
    def dimension_ids(self):
        return (d.id for d in self._cauldron.values() if isinstance(d, Dimension))

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

        for f in filters:
            self._cauldron.use(self._shelf.find(f, (Filter, Having),
                                                constructor=filter_constructor))

        self.dirty = True
        return self

    @property
    def filter_ids(self):
        return (f.id for f in self._filters if isinstance(f, Filter))

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

        # Order bys shouldn't be added to the _cauldron
        self._order_bys = []
        for ingr in order_bys:
            order_by = self._shelf.find(ingr, (Dimension, Metric),
                                        apply_sort_order=True)
            self._order_bys.append(order_by)

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

    def offset(self, offset):
        """ Offset a number of rows before returning rows from the database.

        :param offset: The number of rows to offset in the recipe. 0 will return
                      from the first available row
        :type offset: int
        """
        if self._offset != offset:
            self.dirty = True
            self._offset = offset
        return self

    # ------
    # Utility functions
    # ------

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
                    if str(order_by) not in [str(o) for o in order_bys]:
                        order_bys.add(order_by)

        return list(order_bys)

    def query(self):
        """
        Generates a query using the ingredients supplied by the recipe.

        :return: A SQLAlchemy query
        """
        if not self.dirty and self._query:
            return self._query

        # Step 1: Gather up global filters and user filters and
        # apply them as if they had been added to recipe().filters(...)

        for extension in self.recipe_extensions:
            extension.add_ingredients()

        order_bys = self._prepare_order_bys()

        # Step 2: Build the query (now that it has all the filters
        # and apply any blend recipes

        # Get the parts of the query from the cauldron
        # We don't need to regather order_bys
        columns, group_bys, filters, havings = self._cauldron.brew_query_parts()

        recipe_parts = {
            "columns": columns,
            "group_bys": group_bys,
            "filters": filters,
            "havings": havings,
            "order_bys": order_bys,
        }

        for extension in self.recipe_extensions:
            recipe_parts = extension.modify_recipe_parts(recipe_parts)

        # Start building the query
        query = self._session.query(*recipe_parts['columns'])
        # TODO: .options(FromCache("default"))

        # Only add group_bys at this point because using blend queries
        # may have added more group_bys
        query = query.group_by(*recipe_parts['group_bys']) \
            .order_by(*recipe_parts['order_bys']) \
            .filter(*recipe_parts['filters'])
        if havings:
            for having in recipe_parts['havings']:
                query = query.having(having)

        prequery_parts = {
            "query": query,
            "group_bys": group_bys,
            "filters": filters,
            "havings": havings,
            "order_bys": order_bys,
        }
        for extension in self.recipe_extensions:
            prequery_parts = extension.modify_prequery_parts(prequery_parts)
        query = prequery_parts['query']

        if len(query.selectable.froms) != 1:
            raise BadRecipe("Recipes must use ingredients that all come from "
                            "the same table. \nDetails on this recipe:\n{"
                            "}".format(str(self._cauldron)))

        postquery_parts = {
            "query": query,
            "group_bys": group_bys,
            "filters": filters,
            "havings": havings,
            "order_bys": order_bys,
        }
        for extension in self.recipe_extensions:
            postquery_parts = extension.modify_postquery_parts(postquery_parts)
        query = postquery_parts['query']

        # Apply limit on the outermost query
        # This happens after building the comparison recipe
        if self._limit and self._limit > 0:
            query = query.limit(self._limit)

        if self._offset and self._offset > 0:
            query = query.offset(self._offset)

        # Step 5:  Clear the dirty flag,
        # Patch the query if there's a comparison query
        # cache results

        self._query = query
        self.dirty = False
        return self._query

    @property
    def dirty(self):
        """ The recipe is dirty if it is flagged dirty or any extensions are
        flagged dirty """
        if self._dirty:
            return True
        else:
            for extension in self.recipe_extensions:
                if extension.dirty:
                    return True
        return False

    @dirty.setter
    def dirty(self, value):
        """ If dirty is true set the recipe to dirty flag. If false,
        clear the recipe and all extension dirty flags """
        if value:
            self._dirty = True
        else:
            self._dirty = False
            for extension in self.recipe_extensions:
                extension.dirty = False

    def table(self):
        """ A convenience method to determine the table the query is
        selecting from
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

    def as_table(self, name=None):
        """ Return an alias to a table
        """
        if name is None:
            name = self._id
        return alias(self.subquery(), name=name)

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

    @property
    def dataset(self):
        rows = self.all()
        if rows:
            first_row = rows[0]
            return tablib.Dataset(*rows, headers=first_row._fields)
        else:
            return  tablib.Dataset([], headers=[])


    def first(self):
        """ Return the first element on the result
        """
        return self.one()
