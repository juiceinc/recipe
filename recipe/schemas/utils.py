from datetime import date, datetime
import dateparser
import inspect
from sqlalchemy import distinct, func
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql.base import ImmutableColumnCollection
from sureberus import schema as S

from recipe.exceptions import BadIngredient
from recipe.compat import basestring

SCALAR_TYPES = [S.Integer(), S.String(), S.Float(), S.Boolean()]


def _chain(*args):
    """Chain several coercers together"""

    def fn(value):
        for arg in args:
            value = arg(value)
        return value

    return fn


def _make_sqlalchemy_datatype_lookup():
    """ Build a dictionary of the allowed sqlalchemy casts """
    from sqlalchemy.sql import sqltypes

    d = {}
    for name in dir(sqltypes):
        sqltype = getattr(sqltypes, name)
        if name.lower() not in d and name[0] != "_" and name != "NULLTYPE":
            if inspect.isclass(sqltype) and issubclass(sqltype, sqltypes.TypeEngine):
                d[name.lower()] = sqltype
    return d


sqlalchemy_datatypes = _make_sqlalchemy_datatype_lookup()

format_lookup = {
    "comma": ",.0f",
    "dollar": "$,.0f",
    "percent": ".0%",
    "comma1": ",.1f",
    "dollar1": "$,.1f",
    "percent1": ".1%",
    "comma2": ",.2f",
    "dollar2": "$,.2f",
    "percent2": ".2%",
}


def coerce_format(v):
    return format_lookup.get(v, v)


aggregations = {
    "sum": func.sum,
    "min": func.min,
    "max": func.max,
    "avg": func.avg,
    "count": func.count,
    "count_distinct": lambda fld: func.count(distinct(fld)),
    "month": lambda fld: func.date_trunc("month", fld),
    "week": lambda fld: func.date_trunc("week", fld),
    "year": lambda fld: func.date_trunc("year", fld),
    "quarter": lambda fld: func.date_trunc("quarter", fld),
    "age": lambda fld: func.date_part("year", func.age(fld)),
    "none": lambda fld: fld,
    None: lambda fld: fld,
    # Percentile aggregations do not work in all engines
    "median": func.median,
    "percentile1": lambda fld: func.percentile_cont(0.01).within_group(fld),
    "percentile5": lambda fld: func.percentile_cont(0.05).within_group(fld),
    "percentile10": lambda fld: func.percentile_cont(0.10).within_group(fld),
    "percentile25": lambda fld: func.percentile_cont(0.25).within_group(fld),
    "percentile50": lambda fld: func.percentile_cont(0.50).within_group(fld),
    "percentile75": lambda fld: func.percentile_cont(0.75).within_group(fld),
    "percentile90": lambda fld: func.percentile_cont(0.90).within_group(fld),
    "percentile95": lambda fld: func.percentile_cont(0.95).within_group(fld),
    "percentile99": lambda fld: func.percentile_cont(0.99).within_group(fld),
}


def coerce_pop_version(shelf):
    shelf.pop("_version", None)
    return shelf


class IntelligentDates:
    def __init__(self):
        self.lookup = {
            "mtd": (self._start_of_month(), "now"),
            "qtd": (self._start_of_quarter(), "now"),
            "mtd": (self._start_of_year(), "now"),
        }

    def _start_of_month(self):
        """Start of the current month"""
        n = datetime.utcnow()
        if self.desired_type == 'date':
            return date(n.year, n.month, 1)
        else:
            return datetime(n.year, n.month, 1)

    def _start_of_quarter(self):
        """Start of the current month"""
        n = datetime.utcnow()
        qtr_month = 3 * divmod(n.month, 3)[0] + 1
        if self.desired_type == 'date':
            return date(n.year, qtr_month, 1)
        else:
            return datetime(n.year, qtr_month, 1)

    def _start_of_year(self):
        """Start of the current month"""
        n = datetime.utcnow()
        if self.desired_type == 'date':
            return date(n.year, 1, 1)
        else:
            return datetime(n.year, 1, 1)

    def _convert(self, v):
        if v is None:
            return None
        elif isinstance(v, basestring):
            # Always returns a datetime
            result = dateparser.parse(v)
        else:
            # Callable
            result = v()

        if self.desired_type == 'date' and isinstance(result, datetime):
            return result.date()
        else:
            return result

    def __call__(self, field, value):
        """Convert a value into an intelligent date pair if possible"""
        if str(field.type) == "DATE":
            self.desired_type = 'date'
        else:
            self.desired_type = 'datetime'

        if not isinstance(value, basestring):
            return (None, None)
        else:
            value = value.lower()

            low, high = self.lookup.get(value, (None, None))
            return (self._convert(low), self._convert(high))


def _convert_date_value(v):
    parse_kwargs = {"languages": ["en"]}
    if isinstance(v, date):
        return v
    elif isinstance(v, datetime):
        return v.date()
    elif isinstance(v, basestring):
        parsed_dt = dateparser.parse(v, **parse_kwargs)
        if parsed_dt is None:
            raise ValueError("Could not parse date in {}".format(v))
        return parsed_dt.date()
    else:
        raise ValueError("Can not convert {} to date".format(v))


def _convert_datetime_value(v):
    parse_kwargs = {"languages": ["en"]}
    if isinstance(v, datetime):
        return v
    elif isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    elif isinstance(v, basestring):
        parsed_dt = dateparser.parse(v, **parse_kwargs)
        if parsed_dt is None:
            raise ValueError("Could not parse datetime in {}".format(v))
        return parsed_dt
    else:
        raise ValueError("Can not convert {} to datetime".format(v))


def convert_value(field, value):
    """Convert values into something appropriate for this SQLAlchemy data type

    :param field: A SQLAlchemy expression
    :param values: A value or list of values
    """

    if isinstance(value, (list, tuple)):
        if str(field.type) == "DATE":
            return [_convert_date_value(v) for v in value]
        elif str(field.type) == "DATETIME":
            return [_convert_datetime_value(v) for v in value]
        else:
            return value
    else:
        if str(field.type) == "DATE":
            return _convert_date_value(value)
        elif str(field.type) == "DATETIME":
            return _convert_datetime_value(value)
        else:
            return value


def _find_in_columncollection(columns, name):
    """ Find a column in a column collection by name or _label"""
    for col in columns:
        if col.name == name or getattr(col, "_label", None) == name:
            return col
    return None


def find_column(selectable, name):
    """
    Find a column named `name` in selectable

    :param selectable:
    :param name:
    :return: A column object
    """
    from recipe import Recipe

    if isinstance(selectable, Recipe):
        selectable = selectable.subquery()

    # Selectable is a table
    if isinstance(selectable, DeclarativeMeta):
        col = getattr(selectable, name, None)
        if col is not None:
            return col

        col = _find_in_columncollection(selectable.__table__.columns, name)
        if col is not None:
            return col

    # Selectable is a sqlalchemy subquery
    elif hasattr(selectable, "c") and isinstance(
        selectable.c, ImmutableColumnCollection
    ):
        col = getattr(selectable.c, name, None)
        if col is not None:
            return col

        col = _find_in_columncollection(selectable.c, name)
        if col is not None:
            return col

    raise BadIngredient("Can not find {} in {}".format(name, selectable))


def ingredient_class_for_name(class_name):
    """Get the class in the recipe.ingredients module with the given name."""
    from recipe import ingredients

    return getattr(ingredients, class_name, None)
