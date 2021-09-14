from json import loads
from sqlalchemy import and_, func, text, or_, String
from sqlalchemy.ext.declarative import declarative_base

from recipe.core import Recipe
from recipe.exceptions import BadRecipe
from recipe.ingredients import Dimension, Ingredient, Metric, Filter
from recipe.utils import FakerAnonymizer, recipe_arg, pad_values

Base = declarative_base()


class RecipeExtension(object):
    """
    Recipe extensions plug into the recipe builder pattern and can modify the
    generated query.

    recipe generates a query in the following way

        (RECIPE) recipe checks if a query has been generated

        (EXTENSIONS) all extension ``add_ingredients`` run to inject
        ingredients directly on the recipe

        (RECIPE) recipe runs gather_all_ingredients_into_cauldron to build a
        global lookup for ingredients

        (RECIPE) recipe runs cauldron.brew_query_parts to gather sqlalchemy
        columns, group_bys and filters

        (EXTENSIONS) all extension ``modify_recipe_parts(recipeparts)`` run to
        directly modify the collected sqlalchemy columns, group_bys or filters

        (RECIPE) recipe builds a preliminary query with columns

        (EXTENSIONS) all extension ``modify_prequery_parts(prequery_parts)``
        run to modify the query

        (RECIPE) recipe builds a full query with group_bys, order_bys,
        and filters.

        (RECIPE) recipe tests that this query only uses a single from

        (EXTENSIONS) all extension ``modify_postquery_parts(
        postquery_parts)`` run to modify the query

        (RECIPE) recipe applies limits and offsets on the query

        (RECIPE) recipe caches completed query

    When the recipe fetches data the results will be ``enchanted`` to add
    fields to the result. ``RecipeExtensions`` can modify result rows with

        enchant_add_fields: Return a tuple of field names to add to a
        result row

        enchant_row(row): Return a tuple of field values for each row in
        results.

    """

    def __init__(self, recipe):
        self.recipe = recipe

    def add_ingredients(self):
        """
        Add ingredients to the recipe

        This method should be overridden by subclasses"""
        pass

    def modify_recipe_parts(self, recipe_parts):
        """
        Modify sqlalchemy components of the query

        This method allows extensions to directly modify columns,
        group_bys, filters, and order_bys generated from collected
        ingredients."""
        return {
            "columns": recipe_parts["columns"],
            "group_bys": recipe_parts["group_bys"],
            "filters": recipe_parts["filters"],
            "havings": recipe_parts["havings"],
            "order_bys": recipe_parts["order_bys"],
        }

    def modify_prequery_parts(self, prequery_parts):
        """This method allows extensions to directly modify query,
        group_bys, filters, and order_bys generated from collected
        ingredients after a preliminary query using columns has been created.
        """
        return {
            "query": prequery_parts["query"],
            "group_bys": prequery_parts["group_bys"],
            "filters": prequery_parts["filters"],
            "havings": prequery_parts["havings"],
            "order_bys": prequery_parts["order_bys"],
        }

    def modify_postquery_parts(self, postquery_parts):
        """This method allows extensions to directly modify query,
        group_bys, filters, and order_bys generated from collected
        ingredients after a final query using columns has been created.
        """
        return {
            "query": postquery_parts["query"],
            "group_bys": postquery_parts["group_bys"],
            "filters": postquery_parts["filters"],
            "havings": postquery_parts["havings"],
            "order_bys": postquery_parts["order_bys"],
        }

    def enchant_add_fields(self):
        """This method allows extensions to add fields to a result row.
        Return a tuple of the field names that are being added with
        this method
        """
        return ()

    def enchant_row(self, row):
        """This method adds the fields named in ``enchant_add_fields`` to
        each result row."""
        return ()


def handle_directives(directives, handlers):
    for k, v in directives.items():
        method = handlers.get(k)
        if method is None:
            raise BadRecipe("Directive {} isn't handled".format(k))
        method(v)


class AutomaticFilters(RecipeExtension):
    """Automatic generation and addition of Filters to a recipe.

    Automatic filters take a dictionary of keys and values. For each key in
    the dictionary, if the key is the id of a ``Dimension`` on the shelf,
    a filter will be added to the recipe containing the values.
    """

    recipe_schema = {
        "automatic_filters": {"type": "dict"},
        "include_automatic_filter_keys": {"type": "list", "schema": {"type": "string"}},
        "exclude_automatic_filter_keys": {"type": "list", "schema": {"type": "string"}},
        "apply_automatic_filters": {"type": "boolean"},
    }

    def __init__(self, *args, **kwargs):
        super(AutomaticFilters, self).__init__(*args, **kwargs)
        self.apply = True
        self._automatic_filters = {}
        self.exclude_keys = None
        self.include_keys = None
        self._optimize_redshift = False

    @recipe_arg()
    def from_config(self, obj):
        handle_directives(
            obj,
            {
                "automatic_filters": self.automatic_filters,
                "apply_automatic_filters": self.apply_automatic_filters,
                "include_automatic_filter_keys": lambda v: self.include_automatic_filter_keys(
                    *v
                ),
                "exclude_automatic_filter_keys": lambda v: self.exclude_automatic_filter_keys(
                    *v
                ),
            },
        )

    def _build_compound_filter(self, key, values):
        """Build a filter using a compound key. Compound keys are comma delimited.
        Compound values may either be a list of lists or a list of json encoded lists

        For instance::

            key="state,age"
            values=[["California",22],["Iowa", 24]]

        will generate a filter equal to the following::

            WHERE (state='California' AND age=22) OR
                  (state='Iowa' and age=24)

        Optionally, the values can be a json encoded list.

            key="state,age"
            values=['["California", 22]', '["Iowa", 24]']

        Args:
            key (str): A string containing a comma separated list of ids.
            values (list): A list of lists containing that will be matched to the ids

        Returns:
            A SQLAlchemy boolean expression
        """
        keys = key.split(",")
        or_items = []
        for val in values:
            if isinstance(val, str):
                val = loads(val)
                if not isinstance(val, list):
                    raise ValueError(
                        "Compound filter values must be json encoded lists"
                    )
            and_items = []
            for d, v in zip(keys, val):
                filt = self._build_automatic_filter(d, v)
                if filt is not None:
                    and_items.append(filt)
            if and_items:
                or_items.append(and_(*and_items))
        if or_items:
            return or_(*or_items)
        else:
            return None

    def _build_automatic_filter(self, dim, values):
        """Build an automatic filter given a dim and a value.

        The dim may contain a dimension id and an optional operator
        """
        operator = None
        if "__" in dim:
            dim, operator = dim.split("__")
        if self.include_keys is not None and dim not in self.include_keys:
            # Ignore keys that are not in include_keys
            return None

        if self.exclude_keys is not None and dim in self.exclude_keys:
            # Ignore keys that are in exclude_keys
            return None

        # TODO: If dim can't be found, optionally raise a warning
        dimension = self.recipe._shelf.find(dim, Dimension)
        if (
            self._optimize_redshift
            and dimension is not None
            and operator is None
            and isinstance(values, (list, tuple))
            # The first column is the one that will be filtered
            # limit filtering padding to columns that identify as String
            and isinstance(dimension.columns[0].type, String)
        ):
            values = pad_values(values)

        return dimension.build_filter(values, operator)

    def add_ingredients(self):
        if self.apply:
            for dim, values in self._automatic_filters.items():
                if "," in dim:
                    self.recipe.filters(self._build_compound_filter(dim, values))
                else:
                    filt = self._build_automatic_filter(dim, values)
                    if filt is not None:
                        self.recipe.filters(filt)

    @recipe_arg()
    def optimize_redshift(self, value):
        """Toggles whether automatic filters that filter on lists of strings
        are automatically padded to multiples of 5. Doing so will avoid query
        re-compilation for queries that have approximately the same number
        of filter parameters::

            recipe.optimize_redshift(True)
        """
        self._optimize_redshift = value

    @recipe_arg()
    def apply_automatic_filters(self, value):
        """Toggles whether automatic filters are applied to a recipe. The
        following will disable automatic filters for this recipe::

            recipe.apply_automatic_filters(False)
        """
        self.apply = value

    @recipe_arg()
    def automatic_filters(self, value):
        """Sets a dictionary of automatic filters to apply to this recipe.
        If your recipe uses a shelf that has dimensions 'state' and 'gender'
        you could filter the data to Men in California and New Hampshire with::

            shelf = Shelf({
                'state': Dimension(Census.state),
                'gender': Dimension(Census.gender),
                'population': Metric(func.sum(Census.population)),
            })
            recipe = Recipe(shelf=shelf)
            recipe.dimensions('state').metrics('population').automatic_filters({
                'state': ['California', 'New Hampshire'],
                'gender': 'M'
            })

        Automatic filter keys can optionally include an ``operator``.

        **List operators**

        If the value provided in the automatic_filter dictionary is a list,
        the following operators are available. The default operator is ``in``::

            in (default)
            notin
            quickselect (applies multiple conditions matching the
              named quickselect, quickselects are ORed together)
            between (requires a list of two items)

        **Scalar operators**

        If the value provided in the automatic_filter dictionary is a scalar
        (a string, integer, or number), the following operators are available.
        The default operator is ``eq``::

            eq (equal) (the default)
            ne (not equal)
            lt (less than)
            lte (less than or equal)
            gt (greater than)
            gte (greater than or equal)
            like (SQL LIKE)
            ilike (Case insensitive LIKE)
            quickselect (applies the condition matching the named quickselect)

        **An example using operators**

        Here's an example that filters to states that start with the letters
        A-C::

            shelf = Shelf({
                'state': Dimension(Census.state),
                'gender': Dimension(Census.gender),
                'population': Metric(func.sum(Census.population)),
            })
            recipe = Recipe(shelf=shelf)
            recipe.dimensions('state').metrics('population').automatic_filters({
                'state__lt': 'D'
            })

        **Compound filters**

        If the key provided in the automatic_filter dictionary contains a comma,
        the filters will be treated as compound. Compound operators will be matched
        to the values by splitting the key on the commas then zipping the keys
        to values.

        For instance, you could find newborns in California and 20 year olds in
        New Hampshire with::

            shelf = Shelf({
                'state': Dimension(Census.state),
                'age': Dimension(Census.age),
                'population': Metric(func.sum(Census.population)),
            })
            recipe = Recipe(shelf=shelf)
            recipe.dimensions('state').metrics('population').automatic_filters({
                'state,age': [['California',0], ['New Hampshire',20]]
            })

        This would generate a SQL where clause that looked like::

            WHERE
              (Census.state = 'California' and Census.age = 0) OR
              (Census.state = 'New Hampshire' and Census.age = 20)

        Not all keys need to match in compound filters and may be provided.
        For instance, the following example uses operators and "unbalanced" keys::

            recipe.dimensions('state').metrics('population').automatic_filters({
                'state,age__notin': [['California'], ['New Hampshire',[20,21,22,23]]]
            })

        This would generate a SQL where clause that looked like::

            WHERE
              (Census.state = 'California') OR
              (Census.state = 'New Hampshire' and Census.age NOT IN (20,21,22,23))

        Note: Using large numbers of compound filters is not efficient and
        may generate extremely large SQL.

        """
        assert isinstance(value, dict)
        self._automatic_filters = value

    @recipe_arg()
    def exclude_automatic_filter_keys(self, *keys):
        """A "blacklist" of automatic filter keys to exclude. The following will
        cause ``'state'`` to be ignored if it is present in the
        ``automatic_filters`` dictionary::

            recipe.exclude_automatic_filter_keys('state')
        """

        self.exclude_keys = keys

    @recipe_arg()
    def include_automatic_filter_keys(self, *keys):
        """A "whitelist" of automatic filter keys to use. The following will
        **only use** ``'state'`` for automatic filters regardless of what is
        provided in the automatic_filters dictionary::

            recipe.include_automatic_filter_keys('state')
        """
        self.include_keys = keys


class UserFilters(RecipeExtension):
    """Add automatic filtering."""

    def __init__(self, *args, **kwargs):
        super(UserFilters, self).__init__(*args, **kwargs)


class SummarizeOver(RecipeExtension):

    recipe_schema = {"summarize_over": {"type": "string"}}

    def __init__(self, *args, **kwargs):
        super(SummarizeOver, self).__init__(*args, **kwargs)
        self._summarize_over = None

    @recipe_arg()
    def from_config(self, obj):
        handle_directives(obj, {"summarize_over": self.summarize_over})

    @recipe_arg()
    def summarize_over(self, dimension_key):
        self._summarize_over = dimension_key

    def modify_postquery_parts(self, postquery_parts):
        """
        Take a recipe that has dimensions
        Resummarize it over one of the dimensions returning averages of the
        metrics.
        """
        if self._summarize_over is None:
            return postquery_parts
        assert self._summarize_over in self.recipe.dimension_ids

        # Start with a subquery
        subq = postquery_parts["query"].subquery(name="summarize")

        summarize_over_dim = set(
            (
                self._summarize_over,
                self._summarize_over + "_id",
                self._summarize_over + "_raw",
            )
        )
        dim_column_names = (
            set(dim for dim in self.recipe.dimension_ids)
            .union(set(dim + "_id" for dim in self.recipe.dimension_ids))
            .union(set(dim + "_raw" for dim in self.recipe.dimension_ids))
        )
        used_dim_column_names = dim_column_names - summarize_over_dim

        # Build a new query around the subquery
        group_by_columns = [col for col in subq.c if col.name in used_dim_column_names]

        # Generate columns for the metric, remapping the aggregation function
        # count -> sum
        # sum -> sum
        # avg -> avg
        # Metrics can override the summary aggregation by providing a
        # metric.meta.summary_aggregation callable parameter
        metric_columns = []
        for col in subq.c:
            if col.name not in dim_column_names:
                met = self.recipe._cauldron.find(col.name, Metric)
                summary_aggregation = met.meta.get("summary_aggregation", None)
                if summary_aggregation is None:
                    if str(met.expression).startswith(u"avg"):
                        summary_aggregation = func.avg
                    elif str(met.expression).startswith(u"count"):
                        summary_aggregation = func.sum
                    elif str(met.expression).startswith(u"sum"):
                        summary_aggregation = func.sum

                if summary_aggregation is None:
                    # We don't know how to aggregate this metric in a summary
                    raise BadRecipe(
                        u"Provide a summary_aggregation for metric"
                        u" {}".format(col.name)
                    )
                metric_columns.append(summary_aggregation(col).label(col.name))

        # Find the ordering columns and apply them to the new query
        order_by_columns = []
        for col in postquery_parts["query"]._order_by:
            subq_col = getattr(subq.c, str(col).split(" ")[0])
            if subq_col is not None:
                order_by_columns.append(subq_col)

        postquery_parts["query"] = (
            self.recipe._session.query(*(group_by_columns + metric_columns))
            .group_by(*group_by_columns)
            .order_by(*order_by_columns)
        )

        # Remove the summarized dimension
        self.recipe._cauldron.pop(self._summarize_over, None)
        return postquery_parts


class Anonymize(RecipeExtension):
    """Allows recipes to be anonymized by adding an anonymize property.
    This flips the anonymize flag on all Ingredients used in the recipe.

    Injects an ingredient.meta._anonymize boolean property on each used
    ingredient.

    AnonymizeRecipe should occur last
    """

    recipe_schema = {"anonymize": {"type": "boolean"}}

    def __init__(self, *args, **kwargs):
        super(Anonymize, self).__init__(*args, **kwargs)
        self._anonymize = False

    @recipe_arg()
    def from_config(self, obj):
        handle_directives(obj, {"anonymize": self.anonymize})

    @recipe_arg()
    def anonymize(self, value):
        """Should this recipe be anonymized"""
        assert isinstance(value, bool)
        self._anonymize = value

    def add_ingredients(self):
        """Put the anonymizers in the last position of formatters"""
        for ingredient in self.recipe._cauldron.values():
            if hasattr(ingredient.meta, "anonymizer"):
                anonymizer = ingredient.meta.anonymizer

                # Build a FakerAnonymizer if we have a string
                if isinstance(anonymizer, str):

                    # Check for extra parameters
                    kwargs = {}
                    anonymizer_locale = getattr(
                        ingredient.meta, "anonymizer_locale", None
                    )
                    anonymizer_postprocessor = getattr(
                        ingredient.meta, "anonymizer_postprocessor", None
                    )
                    anonymizer_providers = getattr(
                        ingredient.meta, "anonymizer_providers", None
                    )
                    if anonymizer_postprocessor is not None:
                        kwargs["postprocessor"] = anonymizer_postprocessor
                    if anonymizer_locale is not None:
                        kwargs["locale"] = anonymizer_locale
                    if anonymizer_providers is not None:
                        kwargs["providers"] = anonymizer_providers

                    anonymizer = FakerAnonymizer(anonymizer, **kwargs)

                # Strip out all FakerAnonymizers
                ingredient.formatters = [
                    f
                    for f in ingredient.formatters
                    if not isinstance(f, FakerAnonymizer)
                ]

                if self._anonymize:
                    if ingredient.meta.anonymizer not in ingredient.formatters:
                        ingredient.formatters.append(anonymizer)
                else:
                    if ingredient.meta.anonymizer in ingredient.formatters:
                        ingredient.formatters.remove(anonymizer)


class Paginate(RecipeExtension):
    """
    Allows recipes to paginate results. Pagination also supports
    searching and sorting within paginated data.

    **Using and controlling pagination**

    Pagination returns pages of data using limit and offset.

    Pagination is enabled by setting a nonzero page size, like this::

        shelf = Shelf({
            'state': Dimension(Census.state),
            'gender': Dimension(Census.gender),
            'population': Metric(func.sum(Census.population)),
        })
        recipe = Recipe(shelf=shelf, extension_classes=[Paginate])\
            .dimensions('state')\
            .metrics('population')\
            .pagination_page_size(10)


    Pagination may be disabled by setting `.apply_pagination(False)`.

    **Searching**

    `pagination_q` allows a recipe to be searched for a string.
    The default search fields are all dimensions used in the recipe.
    Search keys can be customized with `pagination_search_keys`.
    Search may be disabled by setting `.apply_pagination_filters(False)`
    The value role will be targetted when searching dimensions.

    **Sorting**

    Pagination can override ordering applied to a recipe by setting
    `.pagination_order_by(...)` to a list of ordering keys. If keys are
    preceded by a "-", ordering is descending, otherwise ordering is ascending.

    **An example using all features**

    Here's an example that searches for keys that start with "t", showing
    the fifth page of results::

        shelf = Shelf({
            'state': Dimension(Census.state),
            'gender': Dimension(Census.gender),
            'age': Dimension(Census.age),
            'population': Metric(func.sum(Census.population)),
        })
        recipe = self.recipe()\
            .metrics("pop2000")\
            .dimensions("state", "sex", "age")\
            .pagination_page_size(10)\
            .pagination_page(5)\
            .pagination_q('t%')\
            .pagination_search_keys("state", "sex")


    This will generate SQL like::

        SELECT census.age AS age,
               census.sex AS sex,
               census.state AS state,
               sum(census.population) AS population
        FROM census
        WHERE lower(census.state) LIKE lower('t%')
          OR lower(census.sex) LIKE lower('t%')
        GROUP BY census.age,
                 census.sex,
                 census.state
        LIMIT 10
        OFFSET 40

    """

    recipe_schema = {
        "apply_pagination": {"type": "boolean"},
        "apply_pagination_filters": {"type": "boolean"},
        "pagination_order_by": {"type": "list", "elements": {"type": "string"}},
        "pagination_default_order_by": {"type": "list", "elements": {"type": "string"}},
        "pagination_q": {"type": "string"},
        "pagination_search_keys": {"type": "list", "elements": {"type": "string"}},
        "pagination_page_size": {"type": "integer"},
        "pagination_page": {"type": "integer"},
    }

    def __init__(self, *args, **kwargs):
        super(Paginate, self).__init__(*args, **kwargs)
        self._apply_pagination = True
        self._apply_pagination_filters = True
        self._pagination_q = ""
        self._paginate_search_keys = []
        self._pagination_order_by = []
        self._pagination_default_order_by = None
        self._pagination_page_size = 0
        self._pagination_page = 1
        self._validated_pagination = None

    @recipe_arg()
    def from_config(self, obj):
        handle_directives(
            obj,
            {
                "apply_pagination": lambda v: self.apply_pagination(v),
                "apply_pagination_filters": lambda v: self.apply_pagination_filters(v),
                "pagination_order_by": lambda v: self.pagination_order_by(*v),
                "pagination_default_order_by": lambda v: self.pagination_default_order_by(
                    *v
                ),
                "pagination_q": lambda v: self.pagination_q(v),
                "pagination_search_keys": lambda v: self.pagination_search_keys(*v),
                "pagination_page_size": lambda v: self.pagination_page_size(v),
                "pagination_page": lambda v: self.pagination_page(v),
            },
        )

    @recipe_arg()
    def apply_pagination(self, value):
        """Should this recipe be paginated.

        :param value: Enable or disable pagination for this recipe, default True
        :type value: bool
        """
        assert isinstance(value, bool)
        self._apply_pagination = value

    @recipe_arg()
    def apply_pagination_filters(self, value):
        """Should this recipe apply the paginations query filtering.

        Should paginate_q be used to apply a search on paginate_search_keys or
        all dimensions used in the recipe.

        :param value: Enable or disable pagination filtering for this recipe, default True
        :type value: bool
        """
        assert isinstance(value, bool)
        self._apply_pagination_filters = value

    @recipe_arg()
    def pagination_order_by(self, *value):
        """Sort this pagination by these keys. Pagination ordering is applied
        before any other order_bys defined in the recipe.

        :param value: A list of keys to order the paginated recipe by
        :type value: list(str)
        """
        assert isinstance(value, (list, tuple))
        self._pagination_order_by = value

    @recipe_arg()
    def pagination_default_order_by(self, *value):
        """Paginated queries must be ordered. This ordering is applied if
        the recipe has no order_by and no pagination_order_by has been set.

        :param value: A list of keys to order the paginated recipe by
          if not other ordering is applied.
        :type value: list(str)
        """
        assert isinstance(value, (list, tuple))
        self._pagination_default_order_by = value

    @recipe_arg()
    def pagination_q(self, value):
        """Search this recipe for this string. The search is an case
        insensitive like that ORs all dimensions in the recipe by default.

        To search for a substring, use a percentage sign for wildcard,
        like '%searchval%'.

        `pagination_search_keys` can be used to customize what keys are used
        for search.

        :param value: A query string to search for this in this recipe.
            The query string is evaluated as a `ilike` on all dimensions
            in the recipe or pagination_search_keys if provided
        :type value: str
        """
        assert isinstance(value, str)
        self._pagination_q = value

    @recipe_arg()
    def pagination_search_keys(self, *value):
        """When querying this recipe with a `pagination_q`, search these keys

        pagination_search_keys do not have to be used in the recipe.

        :param value: A list of keys to search in the paginated recipe
        :type value: list(str)
        """
        assert isinstance(value, (list, tuple))
        self._paginate_search_keys = value

    @recipe_arg()
    def pagination_page_size(self, value):
        """Paginate recipe responses into pages of this size.

        A page size of zero disables pagination.

        :param value: A page size (zero or a positive integer)
        :type value: integer
        """
        assert isinstance(value, int)
        assert value >= 0
        self._pagination_page_size = value

    @recipe_arg()
    def pagination_page(self, value):
        """Fetch this page.

        :param value: A positive integer page number to fetch
        :type value: integer
        """
        assert isinstance(value, int)

        # Pagination page must be a positive integer
        self._pagination_page = max(1, value)

    def do_pagination(self):
        """Should pagination be added to this recipe."""
        return self._apply_pagination and self._pagination_page_size > 0

    def _apply_pagination_order_by(self):
        """Inject pagination ordering ahead of any existing ordering."""

        # Inject the paginator ordering ahead of the existing ordering and filter
        # out sort items that aren't in the cauldron
        if self.do_pagination():

            def make_ordering_key(ingr):
                if isinstance(ingr, Ingredient):
                    if ingr.ordering == "desc":
                        return "-" + ingr.id
                    else:
                        return ingr.id
                else:
                    return ingr

            # Recover the existing orderings
            existing_orderings = [
                make_ordering_key(ingr) for ingr in self.recipe._order_bys
            ]

            # Remove paginator sort keys from any existing order bys
            # Search for both ascending and descending versions of the keys
            existing_order_bys = [
                key
                for key in existing_orderings
                if key not in self._pagination_order_by
                and ("-" + key) not in self._pagination_order_by
            ]
            new_order_by = list(self._pagination_order_by) + existing_order_bys

            if self._pagination_default_order_by is None:
                self._pagination_default_order_by = self.recipe._cauldron.dimension_ids
            if not new_order_by:
                self.recipe.order_by(*self._pagination_default_order_by)
            else:
                self.recipe.order_by(*new_order_by)

    def _apply_pagination_q(self):
        """Apply pagination querying to all paginate search keys"""
        q = self._pagination_q
        if self._apply_pagination_filters and q:
            search_keys = self._paginate_search_keys or self.recipe.dimension_ids

            filters = []
            for key in search_keys:
                # build a filter for each search key and use in the recipe
                ingredient = self.recipe._shelf.get(key, None)
                if ingredient:
                    filters.append(
                        ingredient.build_filter(
                            q, operator="ilike", target_role="value"
                        )
                    )

            # Build a big or filter for the search
            if filters:
                or_expression = or_(
                    f.filters[0] if hasattr(f, "filters") else f for f in filters
                )
                search_filter = Filter(or_expression)
                self.recipe._cauldron.use(search_filter)

    def add_ingredients(self):
        """Apply pagination ordering and search to this query if necessary."""
        self._apply_pagination_order_by()
        self._apply_pagination_q()

    def modify_postquery_parts(self, postquery_parts):
        """Apply validated pagination limits and offset to a completed query."""
        if not self.do_pagination():
            return postquery_parts

        limit = self._pagination_page_size

        # Validate what page we are on by looking at the total
        # number of items.
        total_count = self.recipe.total_count(postquery_parts["query"])

        d, m = divmod(total_count, limit)
        total_pages = max(1, d + (1 if m > 0 else 0))
        page = self._pagination_page
        validated_page = min(max(1, page), total_pages)

        self._validated_pagination = {
            "requestedPage": page,
            "page": validated_page,
            "pageSize": limit,
            "totalItems": total_count,
        }

        # page=1 is the first page
        offset = limit * (validated_page - 1)

        postquery_parts["query"] = postquery_parts["query"].limit(limit)
        if offset:
            postquery_parts["query"] = postquery_parts["query"].offset(offset)

        return postquery_parts

    def validated_pagination(self):
        """Return pagination validated against the actual number of items in the
        response. Returns None if the recipe has not run.
        """
        return self._validated_pagination


class PaginateInline(Paginate):
    """
    Allows recipes to paginate results while returning total record count as a
    field in the recipe itself. PaginateInline differs from Paginate is how
    the recipe behaves when hitting the last page. Because PaginateInline
    only knows the total number of items after a recipe has run, it is possible
    to set a page that goes beyond the total number of results. In this case
    PaginateInline will result in a query that returns 0 results. The pagination
    page in this case will be reset back to the first page and the query will
    run again.

    **Using and controlling pagination**

    Pagination returns pages of data using limit and offset.

    Pagination is enabled by setting a nonzero page size, like this::

        shelf = Shelf({
            'state': Dimension(Census.state),
            'gender': Dimension(Census.gender),
            'population': Metric(func.sum(Census.population)),
        })
        recipe = Recipe(shelf=shelf, extension_classes=[PaginateInline])\
            .dimensions('state')\
            .metrics('population')\
            .pagination_page_size(10)


    Pagination may be disabled by setting `.apply_pagination(False)`.

    **Searching**

    `pagination_q` allows a recipe to be searched for a string.
    The default search fields are all dimensions used in the recipe.
    Search keys can be customized with `pagination_search_keys`.
    Search may be disabled by setting `.apply_pagination_filters(False)`
    The value role will be targetted when searching dimensions.

    **Sorting**

    Pagination can override ordering applied to a recipe by setting
    `.pagination_order_by(...)` to a list of ordering keys. If keys are
    preceded by a "-", ordering is descending, otherwise ordering is ascending.

    **An example using all features**

    Here's an example that searches for keys that start with "t", showing
    the fifth page of results::

        shelf = Shelf({
            'state': Dimension(Census.state),
            'gender': Dimension(Census.gender),
            'age': Dimension(Census.age),
            'population': Metric(func.sum(Census.population)),
        })
        recipe = self.recipe()\
            .metrics("pop2000")\
            .dimensions("state", "sex", "age")\
            .pagination_page_size(10)\
            .pagination_page(5)\
            .pagination_q('t%')\
            .pagination_search_keys("state", "sex")


    This will generate SQL like::

        SELECT census.age AS age,
            census.sex AS sex,
            census.state AS state,
            sum(census.pop2000) AS pop2000,
            min(anon_1._total_count) AS _total_count
        FROM census,

        (SELECT count(*) AS _total_count
         FROM
            (SELECT census.age AS age,
                    census.sex AS sex,
                    census.state AS state,
                    sum(census.pop2000) AS pop2000
            FROM census
            WHERE lower(census.state) LIKE lower('T%')
                OR lower(census.sex) LIKE lower('T%')
            GROUP BY age,
                    sex,
                    state) AS anon_2) AS anon_1

        WHERE lower(census.state) LIKE lower('T%')
        OR lower(census.sex) LIKE lower('T%')
        GROUP BY age,
                sex,
                state
        ORDER BY state,
                sex,
                age
        LIMIT 10
        OFFSET 40

    """

    recipe_schema = {
        "apply_pagination": {"type": "boolean"},
        "apply_pagination_filters": {"type": "boolean"},
        "pagination_order_by": {"type": "list", "elements": {"type": "string"}},
        "pagination_default_order_by": {"type": "list", "elements": {"type": "string"}},
        "pagination_q": {"type": "string"},
        "pagination_search_keys": {"type": "list", "elements": {"type": "string"}},
        "pagination_page_size": {"type": "integer"},
        "pagination_page": {"type": "integer"},
    }

    def modify_postquery_parts(self, postquery_parts):
        """Apply validated pagination limits and offset to a completed query."""
        if not self.do_pagination():
            return postquery_parts

        limit = self._pagination_page_size
        # Get the unvalidated page. When we validate pagination
        # we may need to reset to page 1 if no items are returned.
        validated_page = page = self._pagination_page

        self._validated_pagination = {
            "requestedPage": page,
            "page": page,
            "pageSize": limit,
            "totalItems": 0,
        }

        # page=1 is the first page
        offset = limit * (validated_page - 1)

        postquery_parts["query"] = postquery_parts["query"].limit(limit)
        if offset:
            postquery_parts["query"] = postquery_parts["query"].offset(offset)

        q = postquery_parts["query"]

        # Count the rows in our query without limit or offset or ordering
        total_counter = (
            q.limit(None)
            .offset(None)
            .order_by(None)
            .from_self(func.count().label("_total_count"))
            .subquery()
        )
        # Need an aggregation even though there's only one row
        q = q.add_columns(func.min(total_counter.c._total_count).label("_total_count"))

        postquery_parts["query"] = q
        return postquery_parts

    def validated_pagination(self):
        """Return pagination validated against the actual number of items in the
        response.
        """
        if self.do_pagination():
            validated_pagination = {
                "requestedPage": self._pagination_page,
                "page": self._pagination_page,
                "pageSize": self._pagination_page_size,
                "totalItems": 0,
            }
            rows = self.recipe.all()
            if rows:
                row = rows[0]
                validated_pagination["totalItems"] = row._total_count
            else:
                if self._pagination_page != 1:
                    # Go to the first page and rerun the query
                    self.pagination_page(1)
                    self.recipe.reset()
                    return self.validated_pagination()

            return validated_pagination


class BlendRecipe(RecipeExtension):
    """Add blend recipes, used for joining data from another table to a base
    table

    Supply a second recipe with a different ``from``
    Optionally supply join criteria, if no join criteria is provided join
    will be attempted using constraints.
    All ingredients from the blended recipe will be hoisted to the base
    recipe except for ingredients that are used for joins (they must be the
    same anyway).

    Supports blend (inner) and full_blend (outer) joins.
    """

    def __init__(self, *args, **kwargs):
        super(BlendRecipe, self).__init__(*args, **kwargs)
        self.blend_recipes = []
        self.blend_types = []
        self.blend_criteria = []

    @recipe_arg()
    def blend(self, blend_recipe, join_base, join_blend):
        """Blend a recipe into the base recipe.
        This performs an inner join of the blend_recipe to the
        base recipe's SQL.
        """
        assert isinstance(blend_recipe, Recipe)

        self.blend_recipes.append(blend_recipe)
        self.blend_types.append("inner")
        self.blend_criteria.append((join_base, join_blend))

    @recipe_arg()
    def full_blend(self, blend_recipe, join_base, join_blend):
        """Blend a recipe into the base recipe preserving
        values from both recipes.

        This performs an outer join of the blend_recipe to the
        base recipe."""
        assert isinstance(blend_recipe, Recipe)

        self.blend_recipes.append(blend_recipe)
        self.blend_types.append("outer")
        self.blend_criteria.append((join_base, join_blend))

    def add_ingredients(self):
        """If we have a blend_recipe, modify all ingredients in the base recipe
        to group_by using the direct strategy. This is because when we join
        the base recipe to the blend recipe we will likely have more than one column
        that has the same label. This will generate invalid sql if a more explicit
        reference isn't used."""
        if self.blend_recipes:
            for ingr in self.recipe._cauldron.ingredients():
                if isinstance(ingr, Dimension):
                    ingr.group_by_strategy = "direct"

    def modify_postquery_parts(self, postquery_parts):
        """
        Make the comparison recipe a subquery that is left joined to the
        base recipe using dimensions that are shared between the recipes.

        Hoist the metric from the comparison recipe up to the base query
        while adding the suffix.

        """
        if not self.blend_recipes:
            return postquery_parts

        for blend_recipe, blend_type, blend_criteria in zip(
            self.blend_recipes, self.blend_types, self.blend_criteria
        ):
            join_base, join_blend = blend_criteria

            blend_subq = blend_recipe.subquery()

            # For all metrics in the blend recipe
            # Use the metric in the base recipe and
            # Add the metric columns to the base recipe
            for m in blend_recipe.metric_ids:
                met = blend_recipe._cauldron[m]
                self.recipe._cauldron.use(met)
                for suffix in met.make_column_suffixes():
                    col = getattr(blend_subq.c, met.id, None)
                    if col is not None:
                        postquery_parts["query"] = postquery_parts["query"].add_columns(
                            col.label(met.id + suffix)
                        )
                    else:
                        raise BadRecipe(
                            "{} could not be found in .blend() "
                            "recipe subquery".format(id + suffix)
                        )

            # For all dimensions in the blend recipe
            # Use the dimension in the base recipe and
            # Add the dimension columns and group_by to the base recipe
            # Ignore the join_blend dimension
            for d in blend_recipe.dimension_ids:
                if d == join_blend:
                    continue
                dim = blend_recipe._cauldron[d]
                self.recipe._cauldron.use(dim)
                for suffix in dim.make_column_suffixes():
                    col = getattr(blend_subq.c, dim.id, None)
                    if col is not None:
                        postquery_parts["query"] = postquery_parts["query"].add_columns(
                            col.label(dim.id + suffix)
                        )
                        postquery_parts["query"] = postquery_parts["query"].group_by(
                            col
                        )
                    else:
                        raise BadRecipe(
                            "{} could not be found in .blend() "
                            "recipe subquery".format(id + suffix)
                        )

            base_dim = self.recipe._cauldron[join_base]
            blend_dim = blend_recipe._cauldron[join_blend]

            base_col = base_dim.columns[0]
            blend_col = getattr(blend_subq.c, blend_dim.id_prop, None)
            if blend_col is None:
                raise BadRecipe(
                    "Can't find join property for {} dimension in \
                        blend recipe".format(
                        blend_dim.id_prop
                    )
                )

            if blend_type == "outer":
                postquery_parts["query"] = postquery_parts["query"].outerjoin(
                    blend_subq, base_col == blend_col
                )
            else:
                postquery_parts["query"] = postquery_parts["query"].join(
                    blend_subq, base_col == blend_col
                )

        return postquery_parts


class CompareRecipe(RecipeExtension):
    """Add compare recipes, used for presenting comparative context
    vis-a-vis a base recipe.

    Supply a second recipe with the same ```from``.
    Metrics from the second recipe will be hoisted to the base recipe and
    suffixed with a string (the default is "_compare"
    Dimensions will be used to match the base recipe to the compare recipe.
    Ordering from the base recipe is maintained.
    """

    def __init__(self, *args, **kwargs):
        super(CompareRecipe, self).__init__(*args, **kwargs)
        self.compare_recipe = []
        self.suffix = []

    @recipe_arg()
    def compare(self, compare_recipe, suffix="_compare"):
        """Adds a comparison recipe to a base recipe."""
        assert isinstance(compare_recipe, Recipe)
        assert isinstance(suffix, str)
        self.compare_recipe.append(compare_recipe)
        self.suffix.append(suffix)

    def add_ingredients(self):
        """If we have a compare_recipe, modify all ingredients in the base recipe
        to group_by using the direct strategy. This is because when we join
        the base recipe to the compare recipe we will likely have more than one column
        that has the same label. This will generate invalid sql if a more explicit
        reference isn't used."""
        if self.compare_recipe:
            for ingr in self.recipe._cauldron.ingredients():
                if isinstance(ingr, Dimension):
                    ingr.group_by_strategy = "direct"

    def modify_postquery_parts(self, postquery_parts):
        """Make the comparison recipe a subquery that is left joined to the
        base recipe using dimensions that are shared between the recipes.

        Hoist the metric from the comparison recipe up to the base query
        while adding the suffix.

        """
        if not self.compare_recipe:
            return postquery_parts

        for compare_recipe, compare_suffix in zip(self.compare_recipe, self.suffix):
            comparison_subq = compare_recipe.subquery()

            # For all metrics in the comparison recipe
            # Use the metric in the base recipe and
            # Add the metric columns to the base recipe

            # Comparison metrics hoisted into the base recipe need an
            # aggregation function.The default is func.avg but
            # metrics can override this by provoding a
            # metric.meta.summary_aggregation callable parameter
            for m in compare_recipe.metric_ids:
                met = compare_recipe._cauldron[m]
                id = met.id
                met.id = id + compare_suffix
                summary_aggregation = met.meta.get("summary_aggregation", func.avg)
                self.recipe._cauldron.use(met)
                for suffix in met.make_column_suffixes():
                    col = getattr(comparison_subq.c, id + suffix, None)
                    if col is not None:
                        postquery_parts["query"] = postquery_parts["query"].add_columns(
                            summary_aggregation(col).label(met.id + suffix)
                        )
                    else:
                        raise BadRecipe(
                            "{} could not be found in .compare() "
                            "recipe subquery".format(id + suffix)
                        )

            join_conditions = []
            for dim in compare_recipe.dimension_ids:
                if dim not in self.recipe.dimension_ids:
                    raise BadRecipe(
                        "{} dimension in comparison recipe must exist " "in base recipe"
                    )
                base_dim = self.recipe._cauldron[dim]
                compare_dim = compare_recipe._cauldron[dim]
                base_col = base_dim.columns[0]
                compare_col = getattr(comparison_subq.c, compare_dim.id_prop, None)
                if compare_col is None:
                    raise BadRecipe(
                        "Can't find join property for {} dimension in \
                        compare recipe".format(
                            compare_dim.id_prop
                        )
                    )
                join_conditions.append(base_col == compare_col)

            join_clause = text("1=1")
            if join_conditions:
                join_clause = and_(*join_conditions)

            postquery_parts["query"] = postquery_parts["query"].outerjoin(
                comparison_subq, join_clause
            )

        return postquery_parts
