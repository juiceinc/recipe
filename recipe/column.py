from abc import ABC, abstractmethod
from recipe.schemas.lark_grammar import SQLAlchemyBuilder

from lark.utils import Str
from recipe.utils.datatype import (
    convert_date,
    convert_datetime,
    datatype_from_column_expression,
)
from typing import Dict, List
from functools import total_ordering
from sqlalchemy import cast, String, and_, or_, between


class ColumnStrategy(ABC):
    """
    The Strategy interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    @abstractmethod
    def do_algorithm(self, data: List) -> List:
        pass

    def contribute_to_builder(self) -> Dict:
        """Contribute kwargs to the SQLAlchemy builder"""
        return {}


class DimensionStrategy(ColumnStrategy):

    def contribute_to_builder(self) -> Dict:
        return {"forbid_aggregation": True, "enforce_aggregation": False}


class MetricStrategy(ColumnStrategy):
    """Metrics must be aggregated
    """

    def do_algorithm(self, data: List) -> List:
        return sorted(data)

    def contribute_to_builder(self) -> Dict:
        return {"forbid_aggregation": False, "enforce_aggregation": True}


@total_ordering
class RecipeColumnBase(ABC):
    def __init__(self, label_prefix, expression, **kwargs):
        self.label_prefix = label_prefix
        self.expression = expression
        self.role = kwargs.pop("role", "value").lower()
        self.datatype = kwargs.pop("datatype", None)
        if self.datatype is None:
            self.datatype = datatype_from_column_expression(expression)

    @classmethod
    def from_field(cls, selectable, label_prefix, field):
        builder = SQLAlchemyBuilder.get_builder(selectable)
        expr, datatype = builder.parse(field, **cls.builder_kwargs)
        return cls(label_prefix, expr, datatype=datatype)

    # Order by id, value, (other roles), order_by
    def __lt__(self, other):
        """Sort columns by role. Certain roles always sort first or last"""
        indices = ("id", "value", "*", "order_by")
        try:
            selfidx = indices.index(self.role)
        except:
            selfidx = 2
        try:
            otheridx = indices.index(other.role)
        except:
            otheridx = 2
        if self.idx != otheridx:
            return selfidx < otheridx
        else:
            return self.role < other.role

    def __eq__(self, other):
        """Make ingredients sortable."""
        return self.role == other.role

    def __ne__(self, other):
        """Make ingredients sortable."""
        return not (self.role == other.role)

    def label(self):
        pass

    def select(self, has_formatters=False):
        """What this column contributes to a select """
        pass

    def group_by(self):
        """How does this column contribute to a group_by"""
        pass

    def order_by(self):
        pass

    def filter(self):
        pass

    def having(self):
        pass

    def __str__(self):
        return str(self.expression.compile(compile_kwargs={"literal_binds": True}))

    def _build_scalar_filter(self, value, operator=None):
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

        filter_column = self.expression
        datatype = self.datatype

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
            return filter_column.like(str(value))
        elif operator == "ilike":
            return filter_column.ilike(str(value))
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

    def _build_vector_filter(self, value, operator=None):
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

        filter_column = self.expression
        datatype = self.datatype

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
                        filter_column.isnot(None), filter_column.notin_(non_none_value),
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

    def build_filter(self, value, operator=None):
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

        Returns:

            A SQLAlchemy boolean expression

        """
        value_is_scalar = not isinstance(value, (list, tuple))

        if value_is_scalar:
            return self._build_scalar_filter(value, operator=operator)
        else:
            return self._build_vector_filter(value, operator=operator)


class DimensionColumn(RecipeColumnBase):
    builder_kwargs = {"forbid_aggregation": True, "enforce_aggregation": False}



class MetricColumn(RecipeColumnBase):
    builder_kwargs = {"forbid_aggregation": False, "enforce_aggregation": True}

