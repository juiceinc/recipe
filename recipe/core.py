import logging
import time
import warnings
from copy import copy
from uuid import uuid4

import attr
import tablib
from sqlalchemy import alias, func
from sureberus import normalize_dict, normalize_schema

from recipe.dynamic_extensions import run_hooks
from recipe.exceptions import BadRecipe
from recipe.ingredients import Dimension, Filter, Having, Metric, Ingredient
from recipe.schemas import recipe_schema
from recipe.shelf import Shelf
from recipe.schemas.config_constructors import parse_unvalidated_condition
from recipe.utils import prettyprintable_sql, recipe_arg

ALLOW_QUERY_CACHING = True

warnings.simplefilter("always", DeprecationWarning)

logger = logging.getLogger(__name__)


@attr.s()
class Stats(object):
    rows = attr.ib(default=0)
    dbtime = attr.ib(default=0.0)
    enchanttime = attr.ib(default=0.0)
    from_cache = attr.ib(default=False)


class Recipe(object):
    """A tool for getting data.

    Args:

        shelf (Shelf): A shelf to use for shared metrics
        metrics (:obj:`list` of :obj:`str`)
          A list of metrics to use from
          the shelf. These can also be :obj:`Metric` objects.
        dimensions (:obj:`list` of :obj:`str`)
          A list of dimensions to use
          from the shelf. These can also be :obj:`Dimension` objects.
        filters (:obj:`list` of :obj:`str`)
          A list of filters to use from
          the shelf. These can also be :obj:`Filter` objects.
        order_by (:obj:`list` of :obj:`str`)
          A list of dimension or
          metric keys from the shelf to use for ordering. If prefixed by '-'
          the ordering will be descending.
        session (:obj:`Session`) A SQLAlchemy database session.
        extension_classes (:obj:`list` of :obj:`RecipeExtension`)
          Extensions to apply to this recipe.
        dynamic_extensions (:obj:`list` of :obj:`str`)
          Dynamic extensions to apply to this recipe.

    Returns:
        A Recipe object.
    """

    def __init__(
        self,
        shelf=None,
        metrics=None,
        dimensions=None,
        filters=None,
        order_by=None,
        session=None,
        extension_classes=(),
        dynamic_extensions=None,
    ):
        self._id = str(uuid4())[:8]
        self._query = None
        self._all = None

        self._select_from = None
        self.shelf(shelf)

        # Stores all ingredients used in the recipe
        self._cauldron = Shelf()
        self._order_bys = []

        # Cache options
        self.cache_context = None
        self._cache_region = "default"
        self._cache_prefix = "default"
        self._use_cache = True

        self.stats = Stats()

        # Store the original dimensions and metrics put into the recipe.
        # These may contain duplicates, which will not exist after the
        # ingredients are added to the cauldron.
        self.raw_metrics = tuple()
        self.raw_dimensions = tuple()

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

        self.recipe_extensions = [
            ExtensionClass(self) for ExtensionClass in extension_classes
        ]
        self.dynamic_extensions = dynamic_extensions

    def total_count(self, query=None):
        """Return the number of rows that would be returned by this Recipe,
        ignoring any `limit` that has been applied.

        Args:

            query: An optional SQLAlchemy query to calculate total_count for.
              If None, the recipe query will be used.

        Returns:
            A count of the number of rows that are returned by this query.
        """
        if query is None:
            query = self.query()

        count_query = self._session.query(func.count().label("count")).select_from(
            query.limit(None).offset(None).order_by(None).subquery()
        )

        # If recipe_caching is installed, apply caching to this query.
        try:
            from recipe_caching.mappers import FromCache

            count_query = count_query.options(
                FromCache(self._cache_region, cache_prefix=self._cache_prefix)
            )
        except ImportError:
            pass

        return count_query.scalar()

    def reset(self):
        self._query = None
        self._all = None
        return self

    @classmethod
    def from_config(cls, shelf, obj, **kwargs):
        """
        Construct a Recipe from a plain Python dictionary.

        Most of the directives only support named ingredients, specified as
        strings, and looked up on the shelf. But filters can be specified as
        objects.

        Additionally, each RecipeExtension can extract and handle data from the
        configuration.
        """

        def subdict(d, keys):
            new = {}
            for k in keys:
                if k in d:
                    new[k] = d[k]
            return new

        core_kwargs = subdict(obj, recipe_schema["schema"].keys())
        core_kwargs = normalize_schema(recipe_schema, core_kwargs)
        core_kwargs["filters"] = [
            parse_unvalidated_condition(filter, shelf.Meta.select_from)
            if isinstance(filter, dict)
            else filter
            for filter in obj.get("filters", [])
        ]
        core_kwargs.update(kwargs)
        recipe = cls(shelf=shelf, **core_kwargs)

        # Now let extensions handle their own stuff
        for ext in recipe.recipe_extensions:
            additional_schema = getattr(ext, "recipe_schema", None)
            if additional_schema is not None:
                ext_data = subdict(obj, additional_schema.keys())
                ext_data = normalize_dict(additional_schema, ext_data)
                recipe = ext.from_config(ext_data)
        return recipe

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
            raise AttributeError(
                "{} isn't available on this recipe, "
                "you may need to add an extension".format(name)
            )

        return proxy_callable

    @property
    def metric_ids(self):
        return self._cauldron.metric_ids

    @property
    def dimension_ids(self):
        return self._cauldron.dimension_ids

    @property
    def filter_ids(self):
        return self._cauldron.filter_ids

    @recipe_arg()
    def cache_region(self, value):
        """Set a cache region for recipe-caching to use"""
        assert isinstance(value, str)
        self._cache_region = value

    @recipe_arg()
    def cache_prefix(self, value):
        """Set a cache prefix for recipe-caching to use"""
        assert isinstance(value, str)
        self._cache_prefix = value

    @recipe_arg()
    def use_cache(self, value):
        """If False, invalidate the cache before fetching data."""
        assert isinstance(value, bool)
        self._use_cache = value

    @recipe_arg()
    def shelf(self, shelf=None):
        """Defines a shelf to use for this recipe"""
        if shelf is None:
            self._shelf = Shelf({})
        elif isinstance(shelf, Shelf):
            self._shelf = shelf
        elif isinstance(shelf, dict):
            self._shelf = Shelf(shelf)
        else:
            raise BadRecipe("shelf must be a dict or recipe.shelf.Shelf")

        if self._select_from is None and self._shelf.Meta.select_from is not None:
            self._select_from = self._shelf.Meta.select_from

    @recipe_arg()
    def metrics(self, *metrics):
        """Add a list of Metric ingredients to the query. These can either be
        Metric objects or strings representing metrics on the shelf.

        The Metric expression will be added to the query's select statement.
        The metric value is a property of each row of the result.

        :param metrics: Metrics to add to the recipe. Metrics can
                         either be keys on the ``shelf`` or
                         Metric objects
        :type metrics: list
        """
        self.raw_metrics = self.raw_metrics + copy(metrics)
        for m in metrics:
            self._cauldron.use(self._shelf.find(m, Metric))

    @recipe_arg()
    def dimensions(self, *dimensions):
        """Add a list of Dimension ingredients to the query. These can either be
        Dimension objects or strings representing dimensions on the shelf.

        The Dimension expression will be added to the query's select statement
        and to the group_by.

        :param dimensions: Dimensions to add to the recipe. Dimensions can
                         either be keys on the ``shelf`` or
                         Dimension objects
        :type dimensions: list
        """
        self.raw_dimensions = self.raw_dimensions + copy(dimensions)
        for d in dimensions:
            self._cauldron.use(self._shelf.find(d, Dimension))

    @recipe_arg()
    def filters(self, *filters):
        """
        Add a list of Filter ingredients to the query. These can either be
        Filter objects or strings representing filters on the service's shelf.
        ``.filters()`` are additive, calling .filters() more than once will add
        to the list of filters being used by the recipe.

        The Filter expression will be added to the query's where clause

        :param filters: Filters to add to the recipe. Filters can
                         either be keys on the ``shelf`` or
                         Filter objects or binary expressions
        :type filters: list
        """

        def filter_constructor(f, shelf=None):
            if not isinstance(f, (Filter, Having)) and not isinstance(f, str):
                return Filter(f)
            else:
                return f

        for f in filters:
            if f is not None:
                self._cauldron.use(
                    self._shelf.find(
                        f, (Filter, Having), constructor=filter_constructor
                    )
                )

    @recipe_arg()
    def order_by(self, *order_bys):
        """Apply an ordering to the recipe results.

        :param order_bys: Order_bys to add to the recipe. Order_bys must
                         be keys of ingredients already added to the recipe. If the
                         key is prefixed by "-" the ordering will be descending.
        :type order_bys: list(str)
        """
        # Convert dimensions to use their id
        order_bys = [d.id if isinstance(d, Ingredient) else d for d in order_bys]
        self._order_bys = order_bys

    @recipe_arg()
    def select_from(self, selectable):
        self._select_from = selectable

    @recipe_arg()
    def session(self, session):
        self._session = session

    @recipe_arg()
    def limit(self, limit):
        """Limit the number of rows returned from the database.

        :param limit: The number of rows to return in the recipe. 0 will
                      return all rows.
        :type limit: int
        """
        self._limit = limit

    @recipe_arg()
    def offset(self, offset):
        """Offset a number of rows before returning rows from the database.

        :param offset: The number of rows to offset in the recipe. 0 will
                       return from the first available row
        :type offset: int
        """
        self._offset = offset

    # ------
    # Utility functions
    # ------

    def _is_postgres(self):
        """Determine if the running engine is postgres"""
        try:
            driver = self._session.bind.url.drivername
            if "redshift" in driver or "postg" in driver or "pg" in driver:
                return True
        except:
            pass
        return False

    def _is_redshift(self):
        try:
            driver = self._session.bind.url.drivername
            if "redshift" in driver:
                return True
        except:
            pass
        return False

    def query(self):
        """
        Generates a query using the ingredients supplied by the recipe.

        :return: A SQLAlchemy query
        """
        if self._query is not None:
            return self._query

        if hasattr(self, "optimize_redshift"):
            self.optimize_redshift(self._is_redshift())

        if len(self._cauldron.ingredients()) == 0:
            raise BadRecipe("No ingredients have been added to this recipe")

        # Step 1: Gather up global filters and user filters and
        # apply them as if they had been added to recipe().filters(...)

        for extension in self.recipe_extensions:
            extension.add_ingredients()

        # Step 2: Build the query (now that it has all the filters
        # and apply any blend recipes

        # Get the parts of the query from the cauldron
        # {
        #             "columns": columns,
        #             "group_bys": group_bys,
        #             "filters": filters,
        #             "havings": havings,
        #             "order_bys": list(order_bys)
        #         }
        recipe_parts = self._cauldron.brew_query_parts(self._order_bys)

        for extension in self.recipe_extensions:
            recipe_parts = extension.modify_recipe_parts(recipe_parts)

        # Start building the query
        query = self._session.query(*recipe_parts["columns"])
        if self._select_from is not None:
            query = query.select_from(self._select_from)
        recipe_parts["query"] = (
            query.group_by(*recipe_parts["group_bys"])
            .order_by(*recipe_parts["order_bys"])
            .filter(*recipe_parts["filters"])
        )

        if recipe_parts["havings"]:
            for having in recipe_parts["havings"]:
                recipe_parts["query"] = recipe_parts["query"].having(having)

        for extension in self.recipe_extensions:
            recipe_parts = extension.modify_prequery_parts(recipe_parts)

        if (
            self._select_from is None
            and len(recipe_parts["query"].selectable.froms) != 1
        ):
            raise BadRecipe(
                "Recipes must use ingredients that all come from "
                "the same table. \nDetails on this recipe:\n{"
                "}".format(str(self._cauldron))
            )

        for extension in self.recipe_extensions:
            recipe_parts = extension.modify_postquery_parts(recipe_parts)

        if "recipe" not in recipe_parts:
            recipe_parts["cache_region"] = self._cache_region
            recipe_parts["cache_prefix"] = self._cache_prefix
        recipe_parts = run_hooks(recipe_parts, "modify_query", self.dynamic_extensions)

        # Apply limit on the outermost query
        # This happens after building the comparison recipe
        if self._limit and self._limit > 0:
            recipe_parts["query"] = recipe_parts["query"].limit(self._limit)

        if self._offset and self._offset > 0:
            recipe_parts["query"] = recipe_parts["query"].offset(self._offset)

        # Patch the query if there's a comparison query
        # cache results

        self._query = recipe_parts["query"]
        return self._query

    def _table(self):
        """A convenience method to determine the table the query is
        selecting from
        """
        descriptions = self.query().column_descriptions
        if descriptions:
            return descriptions[0]["entity"]
        else:
            return None

    def to_sql(self):
        """A string representation of the SQL this recipe will generate."""
        return prettyprintable_sql(self.query())

    def subquery(self, name=None):
        """The recipe's query as a subquery suitable for use in joins or other
        queries.
        """
        query = self.query()
        return query.subquery(name=name)

    def as_table(self, name=None):
        """Return an alias to a table"""
        if name is None:
            name = self._id
        return alias(self.subquery(), name=name)

    def all(self):
        """Return a (potentially cached) list of result objects."""
        starttime = fetchtime = enchanttime = time.time()
        self.query()

        if self._all is None:
            fetchtime = time.time()
            if not self._use_cache:
                # Invalidate this query in the cache
                # to ensure it gets fresh data from the database
                if hasattr(self._query, "invalidate"):
                    self._query.invalidate()

            self._all = self._cauldron.enchant(
                self._query.all(), cache_context=self.cache_context
            )
            enchanttime = time.time()
            fetched_from_cache = getattr(self._query, "fetched_from_cache", False)
        else:
            fetched_from_cache = True

        self.stats.rows = len(self._all)
        self.stats.dbtime = fetchtime - starttime
        self.stats.enchanttime = enchanttime - fetchtime
        self.stats.from_cache = fetched_from_cache

        return self._all

    def one(self):
        """Return the first element on the result"""
        all = self.all()
        if len(all) > 0:
            return all[0]
        else:
            return []

    def first(self):
        """Return the first element on the result"""
        return self.one()

    @property
    def dataset(self):
        rows = self.all()
        if rows:
            first_row = rows[0]
            return tablib.Dataset(*rows, headers=first_row._fields)
        else:
            return tablib.Dataset([], headers=[])
