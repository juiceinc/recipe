import attr
from functools import total_ordering
from uuid import uuid4
from sqlalchemy import Float, String, and_, between, case, cast, func, or_, text
from recipe.exceptions import BadIngredient
from recipe.utils import AttrDict, filter_to_string
from recipe.utils.datatype import (
    convert_date,
    convert_datetime,
    determine_datatype,
    datatype_from_column_expression,
)


@total_ordering
class Ingredient(object):
    """Ingredients combine to make a SQLAlchemy query.

    Any unknown keyword arguments provided to an Ingredient
    during initialization are stored in a meta object.

    .. code:: python

        # icon is an unknown keyword argument
        m = Metric(func.sum(MyTable.sales), icon='cog')
        print(m.meta.icon)
        >>> 'cog'

    This meta storage can be used to add new capabilities to
    ingredients.

    Args:

        id (:obj:`str`):
            An id to identify this Ingredient. If ingredients are
            added to a Shelf, the id is automatically set as the key in
            the shelf.
        columns (:obj:`list` of :obj:`ColumnElement`):
            A list of SQLAlchemy columns to use in a query select.
        filters (:obj:`list` of :obj:`BinaryExpression`):
            A list of SQLAlchemy BinaryExpressions to use in the
            .filter() clause of a query.
        havings (:obj:`list` of :obj:`BinaryExpression`):
            A list of SQLAlchemy BinaryExpressions to use in the
            .having() clause of a query.
        columns (:obj:`list` of :obj:`ColumnElement`):
            A list of SQLAlchemy columns to use in the `group_by` clause
            of a query.
        formatters: (:obj:`list` of :obj:`callable`):
            A list of callables to apply to the result values.
            If formatters exist, property `{ingredient.id}_raw` will
            exist on each result row containing the unformatted
            value.
        cache_context (:obj:`str`):
            Extra context when caching this ingredient. DEPRECATED
        ordering (`string`, 'asc' or 'desc'):
            One of 'asc' or 'desc'.  'asc' is the default value.
            The default ordering of this ingredient if it is
            used in a ``recipe.order_by``.
            This is added to the ingredient when the ingredient is
            used in a ``recipe.order_by``.
        group_by_strategy (:obj:`str`):
            A strategy to use when preparing group_bys for the query
            "labels" is the default strategy which will use the labels assigned to
            each column.
            "direct" will use the column expression directly. This alternative is
            useful when there might be more than one column with the same label
            being used in the query.
        quickselects (:obj:`list` of named filters):
            A list of named filters that can be accessed through
            ``build_filter``. Named filters are dictionaries with
            a ``name`` (:obj:str) property and a ``condition`` property
            (:obj:`BinaryExpression`)
        datatype (:obj:`str`):
            The identified datatype (num, str, date, bool, datetime) of
            the parsed expression
        datatype_by_role (:obj:`dict`):
            The identified datatype (num, str, date, bool, datetime) for each
            role.

    Returns:
        An Ingredient object.

    """

    def __init__(self, **kwargs):
        self.id = kwargs.pop("id", uuid4().hex[:12])
        self.columns = kwargs.pop("columns", [])
        self.filters = kwargs.pop("filters", [])
        self.havings = kwargs.pop("havings", [])
        self.group_by = kwargs.pop("group_by", [])
        self.formatters = kwargs.pop("formatters", [])
        self.quickselects = kwargs.pop("quickselects", [])
        self.column_suffixes = kwargs.pop("column_suffixes", None)
        self.cache_context = kwargs.pop("cache_context", "")
        self.datatype = kwargs.pop("datatype", None)
        self.datatype_by_role = kwargs.pop("datatype_by_role", dict())
        self.anonymize = False
        self.roles = {}
        self._labels = []
        self.error = kwargs.pop("error", None)

        # What order should this be in
        self.ordering = kwargs.pop("ordering", "asc")
        self.group_by_strategy = kwargs.pop("group_by_strategy", "labels")

        if not isinstance(self.formatters, (list, tuple)):
            raise BadIngredient(
                "formatters passed to an ingredient must be a list or tuple"
            )
        # If explicit suffixes are passed in, there must be one for each column
        if self.column_suffixes is not None and len(self.column_suffixes) != len(
            self.columns
        ):
            raise BadIngredient("column_suffixes must be the same length as columns")

        # Any remaining passed properties are available in self.meta
        self.meta = AttrDict(kwargs)

    def __hash__(self):
        return hash(self.describe())

    def __repr__(self):
        return self.describe()

    def _stringify(self):
        """Return a relevant string based on ingredient type for repr and
        ordering. Ingredients with the same classname, id and _stringify
        value are considered the same."""
        return " ".join(str(col) for col in self.columns)

    def describe(self):
        """A string representation of the ingredient."""
        return u"({}){} {}".format(self.__class__.__name__, self.id, self._stringify())

    def _format_value(self, value):
        """Formats value using any stored formatters."""
        for f in self.formatters:
            value = f(value)
        return value

    def make_column_suffixes(self):
        """Make sure we have the right column suffixes. These will be appended
        to `id` when generating the query.

        Developers note: These are generated when the query runs because the
        recipe may be run with anonymization on or off, which will inject
        a formatter.
        """
        if self.column_suffixes:
            return self.column_suffixes

        if len(self.columns) == 0:
            return ()

        elif len(self.columns) == 1:
            if self.formatters:
                return ("_raw",)
            else:
                return ("",)
        else:
            raise BadIngredient(
                "column_suffixes must be supplied if there is " "more than one column"
            )

    @property
    def query_columns(self):
        """Yield labeled columns to be used as a select in a query."""
        self._labels = []
        for column, suffix in zip(self.columns, self.make_column_suffixes()):
            self._labels.append(self.id + suffix)
            yield column.label(self.id + suffix)

    @property
    def order_by_columns(self):
        """Yield columns to be used in an order by using this ingredient. Column
        ordering is in reverse order of columns
        """
        # Ensure the labels are generated
        if not self._labels:
            list(self.query_columns)

        if self.group_by_strategy == "labels":
            if self.ordering == "desc":
                suffix = " DESC"
            else:
                suffix = ""

            return [
                text(lbl + suffix)
                for col, lbl in reversed(list(zip(self.columns, self._labels)))
            ]
        else:
            return reversed(self.columns)

    @property
    def cauldron_extras(self):
        """Yield extra tuples containing a field name and a callable that takes
        a row.
        """
        if self.formatters:
            raw_property = self.id + "_raw"
            yield self.id, lambda row: self._format_value(getattr(row, raw_property))

    def _order(self):
        """Ingredients are sorted by subclass then by id."""
        if isinstance(self, Dimension):
            return (0, self.id)
        elif isinstance(self, Metric):
            return (1, self.id)
        elif isinstance(self, Filter):
            return (2, self.id)
        elif isinstance(self, Having):
            return (3, self.id)
        else:
            return (4, self.id)

    def __lt__(self, other):
        """Make ingredients sortable."""
        return self._order() < other._order()

    def __eq__(self, other):
        """Make ingredients sortable."""
        return self._order() == other._order()

    def __ne__(self, other):
        """Make ingredients sortable."""
        return not (self._order() == other._order())

    def _build_scalar_filter(self, value, operator=None, target_role=None):
        """Build a Filter given a single value.

        Args:

            value (a string, number, boolean or None):
            operator (`str`)
                A valid scalar operator. The default operator
                is `eq`
            target_role (`str`)
                An optional role to build the filter against

        Returns:

            A Filter object
        """

        if operator is None:
            operator = "eq"
        if target_role and target_role in self.roles:
            filter_column = self.roles.get(target_role)
            datatype = determine_datatype(self, target_role)
        else:
            filter_column = self.columns[0]
            datatype = determine_datatype(self)

        # Ensure that the filter_column and value have compatible data types

        # Support passing ILIKE in Paginate extensions
        if datatype == "date":
            value = convert_date(value)
        elif datatype == "datetime":
            value = convert_datetime(value)

        if isinstance(value, str) and datatype != "str":
            filter_column = cast(filter_column, String)

        if operator == "eq":
            # Default operator is 'eq' so if no operator is provided, handle
            # like an 'eq'
            if value is None:
                return filter_column.is_(value)
            else:
                return filter_column == value
        if operator == "ne":
            return filter_column != value
        elif operator == "lt":
            return filter_column < value
        elif operator == "lte":
            return filter_column <= value
        elif operator == "gt":
            return filter_column > value
        elif operator == "gte":
            return filter_column >= value
        elif operator == "is":
            return filter_column.is_(value)
        elif operator == "isnot":
            return filter_column.isnot(value)
        elif operator == "like":
            value = str(value)
            return filter_column.like(value)
        elif operator == "ilike":
            value = str(value)
            return filter_column.ilike(value)
        elif operator == "quickselect":
            for qs in self.quickselects:
                if qs.get("name") == value:
                    return qs.get("condition")
            raise ValueError(
                "quickselect {} was not found in "
                "ingredient {}".format(value, self.id)
            )
        else:
            raise ValueError("Unknown operator {}".format(operator))

    def _build_vector_filter(self, value, operator=None, target_role=None):
        """Build a Filter given a list of values.

        Args:

            value (a list of string, number, boolean or None):
            operator (:obj:`str`)
                A valid vector operator. The default operator is
                `in`.
            target_role (`str`)
                An optional role to build the filter against

        Returns:

            A Filter object
        """
        if operator is None:
            operator = "in"
        if target_role and target_role in self.roles:
            filter_column = self.roles.get(target_role)
            datatype = determine_datatype(self, target_role)
        else:
            filter_column = self.columns[0]
            datatype = determine_datatype(self)

        if datatype == "date":
            value = list(map(convert_date, value))
        elif datatype == "datetime":
            value = list(map(convert_datetime, value))

        if operator == "in":
            # Default operator is 'in' so if no operator is provided, handle
            # like an 'in'
            if None in value:
                # filter out the Nones
                non_none_value = sorted([v for v in value if v is not None])
                if non_none_value:
                    return or_(
                        filter_column.is_(None), filter_column.in_(non_none_value)
                    )
                else:
                    return filter_column.is_(None)
            else:
                # Sort to generate deterministic query sql for caching
                value = sorted(value)
                return filter_column.in_(value)

        elif operator == "notin":
            if None in value:
                # filter out the Nones
                non_none_value = sorted([v for v in value if v is not None])
                if non_none_value:
                    return and_(
                        filter_column.isnot(None),
                        filter_column.notin_(non_none_value),
                    )
                else:
                    return filter_column.isnot(None)
            else:
                # Sort to generate deterministic query sql for caching
                value = sorted(value)
                return filter_column.notin_(value)
        elif operator == "between":
            if len(value) != 2:
                ValueError(
                    "When using between, you can only supply a "
                    "lower and upper bounds."
                )
            lower_bound, upper_bound = value
            return between(filter_column, lower_bound, upper_bound)
        elif operator == "quickselect":
            qs_conditions = []
            for v in value:
                qs_found = False
                for qs in self.quickselects:
                    if qs.get("name") == v:
                        qs_found = True
                        qs_conditions.append(qs.get("condition"))
                        break
                if not qs_found:
                    raise ValueError(
                        "quickselect {} was not found in "
                        "ingredient {}".format(value, self.id)
                    )
            return or_(*qs_conditions)
        else:
            raise ValueError("Unknown operator {}".format(operator))

    def build_filter(self, value, operator=None, target_role=None):
        """
        Builds a filter based on a supplied value and optional operator. If
        no operator is supplied an ``in`` filter will be used for a list and a
        ``eq`` filter if we get a scalar value.

        ``build_filter`` is used by the AutomaticFilter extension.

        Args:

            value:
                A value or list of values to operate against
            operator (:obj:`str`)
                An operator that determines the type of comparison
                to do against value.

                The default operator is 'in' if value is a list and
                'eq' if value is a string, number, boolean or None.
            target_role (`str`)
                An optional role to build the filter against

        Returns:

            A SQLAlchemy boolean expression

        """
        value_is_scalar = not isinstance(value, (list, tuple))

        if value_is_scalar:
            return self._build_scalar_filter(
                value, operator=operator, target_role=target_role
            )
        else:
            return self._build_vector_filter(
                value, operator=operator, target_role=target_role
            )

    @property
    def expression(self):
        """An accessor for the SQLAlchemy expression representing this
        Ingredient."""
        if self.columns:
            return self.columns[0]
        else:
            return None


class Filter(Ingredient):
    """A simple filter created from a single expression."""

    def __init__(self, expression, **kwargs):
        super(Filter, self).__init__(**kwargs)
        self.filters = [expression]
        self.datatype = "bool"

    def _stringify(self):
        return filter_to_string(self)

    @property
    def expression(self):
        """An accessor for the SQLAlchemy expression representing this
        Ingredient."""
        if self.filters:
            return self.filters[0]
        else:
            return None


class Having(Ingredient):
    """A Having that limits results based on an aggregate boolean clause"""

    def __init__(self, expression, **kwargs):
        super(Having, self).__init__(**kwargs)
        self.havings = [expression]
        self.datatype = "bool"

    def _stringify(self):
        return " ".join(str(expr) for expr in self.havings)

    @property
    def expression(self):
        """An accessor for the SQLAlchemy expression representing this
        Ingredient."""
        if self.havings:
            return self.havings[0]
        else:
            return None


class Dimension(Ingredient):
    """A Dimension is an Ingredient that adds columns and groups by those
    columns. Columns should be non-aggregate SQLAlchemy expressions.

    The required expression supplies the dimension's "value" role. Additional
    expressions can be provided in keyword arguments with keys
    that look like "{role}_expression". The role is suffixed to the
    end of the SQL column name.

    For instance, the following

    .. code:: python

        Dimension(Hospitals.name,
                  latitude_expression=Hospitals.lat
                  longitude_expression=Hospitals.lng,
                  id='hospital')

    would add columns named "hospital", "hospital_latitude", and
    "hospital_longitude" to the recipes results. All three of these expressions
    would be used as group bys.

    Two special roles that can be added are "id" and "order_by". If a keyword argument
    "id_expression" is passed, this expression will appear first in the list of
    columns and group_bys. This "id" will be used if you call `build_filter` on the
    dimension.

    If the keyword argument "order_by_expression" is passed, this expression will
    appear last in the list of columns and group_bys.

    The following additional keyword parameters are also supported:

    Args:

        lookup (:obj:`dict`):
            A dictionary that is used to map values to new values.

            Note: Lookup adds a ``formatter`` callable as the first
            item in the list of formatters.
        lookup_default (:obj:`object`)
            A default to show if the value can't be found in the
            lookup dictionary.

    Returns:

        A Filter object
    :param lookup: dict A dictionary to translate values into
    :param lookup_default: A default to show if the value can't be found in the
      lookup dictionary.
    """

    def __init__(self, expression, **kwargs):
        super(Dimension, self).__init__(**kwargs)
        if self.datatype is None:
            self.datatype = datatype_from_column_expression(expression)

        # We must always have a value role
        self.roles = {"value": expression}

        for k, v in kwargs.items():
            role = None
            if k.endswith("_expression"):
                # Remove _expression to get the role
                role = k[:-11]
            if role:
                if role == "raw":
                    raise BadIngredient("raw is a reserved role in dimensions")
                self.roles[role] = v

        if not self.datatype_by_role:
            for k, expr in self.roles.items():
                self.datatype_by_role[k] = datatype_from_column_expression(expr)

        self.columns = []
        self._group_by = []
        self.role_keys = []
        if "id" in self.roles:
            self.columns.append(self.roles["id"])
            self._group_by.append(self.roles["id"])
            self.role_keys.append("id")
        if "value" in self.roles:
            self.columns.append(self.roles["value"])
            self._group_by.append(self.roles["value"])
            self.role_keys.append("value")

        # Add all the other columns in sorted order of role
        # with order_by coming last
        # For instance, if the following are passed
        # expression, id_expression, order_by_expresion, zed_expression the order of
        # columns would be "id", "value", "zed", "order_by"
        # When using group_bys for ordering we put them in reverse order.
        ordered_roles = [
            k for k in sorted(self.roles.keys()) if k not in ("id", "value")
        ]
        # Move order_by to the end
        if "order_by" in ordered_roles:
            ordered_roles.remove("order_by")
            ordered_roles.append("order_by")

        for k in ordered_roles:
            self.columns.append(self.roles[k])
            self._group_by.append(self.roles[k])
            self.role_keys.append(k)

        if "lookup" in kwargs:
            self.lookup = kwargs.get("lookup")
            if not isinstance(self.lookup, dict):
                raise BadIngredient("lookup must be a dictionary")
            # Inject a formatter that performs the lookup
            if "lookup_default" in kwargs:
                self.lookup_default = kwargs.get("lookup_default")
                self.formatters.insert(
                    0, lambda value: self.lookup.get(value, self.lookup_default)
                )
            else:
                self.formatters.insert(0, lambda value: self.lookup.get(value, value))

    @property
    def group_by(self):
        # Ensure the labels are generated
        if not self._labels:
            list(self.query_columns)

        if self.group_by_strategy == "labels":
            return [lbl for gb, lbl in zip(self._group_by, self._labels)]
        else:
            return self._group_by

    @group_by.setter
    def group_by(self, value):
        self._group_by = value

    @property
    def cauldron_extras(self):
        """Yield extra tuples containing a field name and a callable that takes
        a row
        """
        # This will format the value field
        for extra in super(Dimension, self).cauldron_extras:
            yield extra

        yield self.id + "_id", lambda row: getattr(row, self.id_prop)

    def make_column_suffixes(self):
        """Make sure we have the right column suffixes. These will be appended
        to `id` when generating the query.
        """
        if self.formatters:
            value_suffix = "_raw"
        else:
            value_suffix = ""

        return tuple(
            value_suffix if role == "value" else "_" + role for role in self.role_keys
        )

    @property
    def id_prop(self):
        """The label of this dimensions id in the query columns"""
        if "id" in self.role_keys:
            return self.id + "_id"
        else:
            # Use the value dimension
            if self.formatters:
                return self.id + "_raw"
            else:
                return self.id


class IdValueDimension(Dimension):
    """
    DEPRECATED: A convenience class for creating a Dimension
    with a separate ``id_expression``.  The following are identical.

    .. code:: python

        d = Dimension(Student.student_name, id_expression=Student.student_id)

        d = IdValueDimension(Student.student_id, Student.student_name)

    The former approach is recommended.

    Args:

        id_expression (:obj:`ColumnElement`)
            A column expression that is used to identify the id
            for a Dimension
        value_expression (:obj:`ColumnElement`)
            A column expression that is used to identify the value
            for a Dimension

    """

    def __init__(self, id_expression, value_expression, **kwargs):
        kwargs["id_expression"] = id_expression
        super(IdValueDimension, self).__init__(value_expression, **kwargs)


class LookupDimension(Dimension):
    """DEPRECATED Returns the expression value looked up in a lookup dictionary"""

    def __init__(self, expression, lookup, **kwargs):
        """A Dimension that replaces values using a lookup table.

        :param expression: The dimension field
        :type value: object
        :param lookup: A dictionary of key/value pairs. If the keys will
           be replaced by values in the value of this Dimension
        :type operator: dict
        :param default: The value to use if a dimension value isn't
           found in the lookup table.  The default behavior is to
           show the original value if the value isn't found in the
           lookup table.
        :type default: object
        """
        if "default" in kwargs:
            kwargs["lookup_default"] = kwargs.pop("default")
        kwargs["lookup"] = lookup

        super(LookupDimension, self).__init__(expression, **kwargs)


class Metric(Ingredient):
    """A simple metric created from a single expression"""

    def __init__(self, expression, **kwargs):
        super(Metric, self).__init__(**kwargs)
        self.columns = [expression]
        if self.datatype is None:
            self.datatype = datatype_from_column_expression(expression)

        # We must always have a value role
        self.roles = {"value": expression}

    def build_filter(self, value, operator=None):
        """Building filters with Metric returns Having objects."""
        f = super().build_filter(value, operator=operator)
        return Having(f.filters[0])


class DivideMetric(Metric):
    """A metric that divides a numerator by a denominator handling several
    possible error conditions

    The default strategy is to add an small value to the denominator
    Passing ifzero allows you to give a different value if the denominator is
    zero.
    """

    def __init__(self, numerator, denominator, **kwargs):
        ifzero = kwargs.pop("ifzero", "epsilon")
        epsilon = kwargs.pop("epsilon", 0.000000001)
        if ifzero == "epsilon":
            # Add an epsilon value to denominator to avoid divide by zero
            # errors
            expression = cast(numerator, Float) / (
                func.coalesce(cast(denominator, Float), 0.0) + epsilon
            )
        else:
            # If the denominator is zero, return the ifzero value otherwise do
            # the division
            expression = case(
                ((cast(denominator, Float) == 0.0, ifzero),),
                else_=cast(numerator, Float) / cast(denominator, Float),
            )
        super(DivideMetric, self).__init__(expression, **kwargs)


class WtdAvgMetric(DivideMetric):
    """A metric that generates the weighted average of a metric by a weight."""

    def __init__(self, expression, weight_expression, **kwargs):
        numerator = func.sum(expression * weight_expression)
        denominator = func.sum(weight_expression)
        super(WtdAvgMetric, self).__init__(numerator, denominator, **kwargs)


class InvalidIngredient(Ingredient):
    pass
