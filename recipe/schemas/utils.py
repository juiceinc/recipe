from copy import copy
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import dateparser
import inspect
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql.base import ImmutableColumnCollection
from sureberus import schema as S
from recipe.exceptions import InvalidColumnError

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
    "comma2": ",.2f",
    "comma3": ",.3f",
    "dollar1": "$,.1f",
    "dollar2": "$,.2f",
    "percent1": ".1%",
    "percent2": ".2%",
    "percent3": ".3%",
}


def coerce_format(v):
    return format_lookup.get(v, v)


def generate_lookup_by_engine(lookup_by_engine, engine):
    """Convert `aggregations_by_engine` or `conversions_by_engine`
    into a dictionary specific to the provided engine/drivername.
    """
    result = copy(lookup_by_engine.get("default", {}))
    result.update(lookup_by_engine.get(engine, {}))
    for k, v in list(result.items()):
        if v is None:
            result.pop(k, None)
    return result


def convert_by_engine_keys_to_regex(lookup_by_engine):
    """ Convert all the keys in a lookup_by_engine to a regex """
    keys = set()
    for d in lookup_by_engine.values():
        for k in d.keys():
            if isinstance(k, str):
                keys.add(k)

    keys = list(keys)
    keys.sort(key=lambda item: (len(item), item), reverse=True)
    return "|".join(keys)


def coerce_pop_version(shelf):
    shelf.pop("_version", None)
    return shelf


def coerce_shelf_meta(shelf):
    """Move shelf meta from the shelf to all ingredients """
    shelf_meta = shelf.pop("_meta", None)
    for k, v in shelf.items():
        # Add shelf meta to all ingredient definitions
        if shelf_meta and not k.startswith("_"):
            v["_shelf_meta"] = copy(shelf_meta)
    return shelf


def _convert_date_value(v):
    parse_kwargs = {"languages": ["en"]}
    if isinstance(v, date):
        return v
    elif isinstance(v, datetime):
        return v.date()
    elif isinstance(v, str):
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
    elif isinstance(v, str):
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

    raise InvalidColumnError(column_name=name)


def ingredient_class_for_name(class_name):
    """Get the class in the recipe.ingredients module with the given name."""
    from recipe import ingredients

    return getattr(ingredients, class_name, None)


def date_offset(dt, offset, **offset_params):
    if offset in ("prior", "previous", "last"):
        dt -= relativedelta(**offset_params)
        return dt
    elif offset == "next":
        dt += relativedelta(**offset_params)
        return dt
    elif offset in ("current", "this"):
        return dt
    else:
        raise ValueError("Unknown intelligent date offset")


def convert_to_start_datetime(dt):
    """Convert a date or datetime to the first moment of the day """
    # Convert a date or datetime to the first moment of the day
    # only if the datetime is the first moment of the day
    if isinstance(dt, (date, datetime)):
        dt = datetime(dt.year, dt.month, dt.day)
    return dt


def convert_to_end_datetime(dt):
    """Convert a date or datetime to the last moment of the day """
    if isinstance(dt, (date, datetime)):
        dt = datetime(dt.year, dt.month, dt.day)
        dt += relativedelta(days=1, microseconds=-1)
    return dt


def convert_to_eod_datetime(dt):
    """Convert a date or datetime to the last moment of the day,
    only convert datetimes if they are the first moment of the day,"""
    if isinstance(dt, datetime):
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            dt += relativedelta(days=1, microseconds=-1)
    elif isinstance(dt, date):
        dt = datetime(dt.year, dt.month, dt.day)
        dt += relativedelta(days=1, microseconds=-1)
    return dt


def calc_date_range(offset, units, dt):
    """Create an intelligent date range using offsets, units and a starting date

    Args:

        offset: An offset
            current|this options for the current period
            prior|previous|last options for the previous period
            next options the next period
        units: The kind of date range to create
            year: A full year
            ytd: The year up to the provided date
            qtr: The full quarter the date belongs to
            month: The full month of the provided date
            mtd: The month up to the provided date
            day: The provided date
        dt
            The date that will be used for calculations

    Returns:

        A tuple of dates constructed using the offsets and units
    """
    offset = str(offset).lower()
    units = str(units).lower()

    # TODO: Add a week unit
    if units == "year":
        dt = date_offset(dt, offset, years=1)
        return date(dt.year, 1, 1), date(dt.year, 12, 31)
    elif units == "ytd":
        dt = date_offset(dt, offset, years=1)
        return date(dt.year, 1, 1), dt
    elif units == "qtr":
        dt = date_offset(dt, offset, months=3)
        qtr = (dt.month - 1) // 3  # Calculate quarter as 0,1,2,3
        return (
            date(dt.year, qtr * 3 + 1, 1),
            date(dt.year, qtr * 3 + 3, 1) + relativedelta(months=1, days=-1),
        )
    elif units == "month":
        dt = date_offset(dt, offset, months=1)
        start_dt = date(dt.year, dt.month, 1)
        return start_dt, start_dt + relativedelta(months=1, days=-1)
    elif units == "mtd":
        dt = date_offset(dt, offset, months=1)
        return date(dt.year, dt.month, 1), dt
    elif units == "day":
        dt = date_offset(dt, offset, days=1)
        return dt, dt
    else:
        raise ValueError("Unknown intelligent date units")
