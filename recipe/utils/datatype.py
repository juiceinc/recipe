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


def datatype_from_column_expression(c):
    """Determine a datatype from a column or column expression"""
    datatype = "unknown"
    try:
        if hasattr(c, "type") and c.type:
            # Check supported column types
            if isinstance(c.type, String):
                datatype = "str"
            elif isinstance(c.type, Date):
                datatype = "date"
            elif isinstance(c.type, DateTime):
                datatype = "datetime"
            elif isinstance(c.type, (Integer, Numeric)):
                datatype = "num"
            elif isinstance(c.type, Boolean):
                datatype = "bool"
    except CompileError:
        pass

    if datatype == "unknown":
        col_expr = str(c).lower()
        if col_expr.startswith("date"):
            datatype = "date"
        elif col_expr.startswith("timestamp"):
            datatype = "datetime"
    return datatype


def determine_datatype(ingr, role="value"):
    """Determine the datatype of an ingredients role

    Developers note: For ingredients constructed from parsed expressions
    this will use the datatype determined by the parser. This will be accurate.
    The fallback for ingredients constructed from raw SQLAlchemy will
    be to examine the column.type or compile the expression and try to
    make inference. This is less accurate.
    """
    from recipe.ingredients import Dimension, Filter, Having, Metric

    if isinstance(ingr, Dimension) and ingr.datatype_by_role:
        return ingr.datatype_by_role.get(role, None)
    elif isinstance(ingr, (Filter, Having)):
        return "bool"
    elif isinstance(ingr, (Metric)):
        return ingr.datatype
