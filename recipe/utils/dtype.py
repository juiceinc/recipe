from datetime import date, datetime
from time import gmtime

import dateparser
from sqlalchemy import Boolean, Date, DateTime, Integer, Numeric, String
from sqlalchemy.exc import CompileError


def convert_date(v):
    """Convert a passed parameter to a date if possible"""
    if v is None:
        return v
    elif isinstance(v, str):
        try:
            dt = dateparser.parse(v)
            if dt is not None:
                return dt.date()
            else:
                return v
        except ValueError:
            return v
    elif isinstance(v, (float, int)):
        # Convert to a date
        tm = gmtime(v)
        return date(tm.tm_year, tm.tm_mon, tm.tm_mday)
    else:
        return v


def convert_datetime(v):
    """Convert a passed parameter to a datetime if possible"""
    if v is None:
        return v
    elif isinstance(v, str):
        try:
            dt = dateparser.parse(v)
            if dt is not None:
                return dt
            else:
                return v
        except ValueError:
            return v
    elif isinstance(v, (float, int)):
        # Convert to a date
        return datetime.utcfromtimestamp(v)
    else:
        return v


def dtype_from_column(c):
    """[summary]

    Args:
        c ([type]): [description]
    """
    if hasattr(c, "type"):
        # Check supported column types
        if isinstance(c.type, String):
            prefix = "str"
        elif isinstance(c.type, Date):
            prefix = "date"
        elif isinstance(c.type, DateTime):
            prefix = "datetime"
        elif isinstance(c.type, Integer):
            prefix = "num"
        elif isinstance(c.type, Numeric):
            prefix = "num"
        elif isinstance(c.type, Boolean):
            prefix = "bool"
        else:
            prefix = "unusable"
    else:
        return "unknown"


def determine_dtype(ingr, c, role):
    """Determine the datatype of a SQLAlchemy column expression

    Developers note: This only works in the simplest of cases. Better
    type identification should be available for lark parsed ingredients.
    """
    if ingr.dtype is not None:
        return ingr.dtype

    try:
        if hasattr(c, "type"):
            col_type = str(c.type).upper()
        else:
            col_type = None
    except CompileError:
        # Some SQLAlchemy expressions don't have a defined type
        col_expr = str(c).lower()
        if col_expr.startswith("date_trunc"):
            col_type = "date"
        elif col_expr.startswith("timestamp_trunc"):
            col_type = "datetime"
        else:
            col_type = "string"

    return col_type

