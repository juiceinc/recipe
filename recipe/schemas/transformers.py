from datetime import date, datetime

import dateparser
import structlog
from lark import GrammarError, Transformer, Tree, v_args
from lark.lexer import Token
from sqlalchemy import (
    Float,
    Integer,
    String,
    and_,
    between,
    case,
    cast,
    distinct,
    func,
    not_,
    or_,
    text,
)

from . import engine_support
from .expression_grammar import ColCollection
from .utils import (
    calc_date_range,
    convert_to_end_datetime,
    convert_to_eod_datetime,
    convert_to_start_datetime,
    literal_0,
    literal_1,
)

SLOG = structlog.get_logger(__name__)


@v_args(inline=True)  # Affects the signatures of the methods
class TransformToSQLAlchemyExpression(Transformer):
    """Converts a field to a SQLAlchemy expression"""

    def __init__(
        self,
        selectable,
        cc: ColCollection,
        drivername: str,
        forbid_aggregation: bool = True,
    ):
        self.text = None
        self.selectable = selectable

        # A dict from column rule name to sqlalchemy column
        self.column_lookup = cc.column_lookup()
        self.last_datatype = None
        # Convert all dates with this conversion
        self.convert_dates_with = None
        self.convert_datetimes_with = None
        self.forbid_aggregation = forbid_aggregation
        self.drivername = drivername

    def _raise_error(self, message):
        tree = None
        tok = None
        # Find the first token
        while tree and tree.children:
            tree = tree.children[0]
            if isinstance(tree, Token):
                tok = tree
                break

        if tok:
            extra_context = self._get_context_for_token(tok)
            message = f"{message}\n{extra_context}"
        raise GrammarError(message)

    def _get_context_for_token(self, tok, span=40):
        pos = tok.start_pos
        start = max(pos - span, 0)
        end = pos + span
        before = self.text[start:pos].rsplit("\n", 1)[-1]
        after = self.text[pos:end].split("\n", 1)[0]
        return before + after + "\n" + " " * len(before) + "^\n"

    def col(self, v):
        return v

    def string(self, v):
        return self.column_lookup[v.data] if isinstance(v, Tree) else v

    def string_cast(self, _, fld):
        """Cast a field to a string"""
        return cast(fld, String())

    def string_substr(self, _, fld, *args):
        """Substring a string. This can take one or two args."""
        if self.drivername.startswith("mssql"):
            if len(args) != 2:
                raise GrammarError("mssql requires a starting number and a length")
            return func.substring(fld, *args)

        # Sqlite, postgres, bigquery, snowflake all accept an
        # optional second argument for length
        return func.substr(fld, *args)

    def num(self, v):
        return self.column_lookup[v.data] if isinstance(v, Tree) else v

    def int_cast(self, _, fld):
        """Cast a field to a string"""
        return cast(fld, Integer())

    def coalesce(self, coalesce, left, right):
        """Coalesce a number, string, date or datetime"""
        return func.coalesce(left, right)

    def boolean(self, v):
        return self.column_lookup[v.data] if isinstance(v, Tree) else v

    def num_add(self, a, b):
        """Add numbers or strings"""
        return a + b

    def string_add(self, a, b):
        """Add numbers or strings"""
        return a + b

    def num_div(self, num, denom):
        """SQL safe division"""
        if not isinstance(denom, (int, float)):
            return (
                case([(denom == 0, None)], else_=num / cast(denom, Float))
                if isinstance(num, (int, float))
                else case(
                    [(denom == 0, None)], else_=cast(num, Float) / cast(denom, Float)
                )
            )

        if denom == 0:
            raise GrammarError("When dividing, the denominator can not be zero")
        elif denom == 1:
            return num
        elif isinstance(num, (int, float)):
            return num / denom
        else:
            return cast(num, Float) / denom

    def num_sub(self, a, b):
        return a - b

    def num_mul(self, a, b):
        return a * b

    def age_conv(self, _, fld):
        """Convert a date to an age"""
        if self.drivername == "bigquery":
            return engine_support.bq_age(fld)
        elif self.drivername == "sqlite":
            raise GrammarError("Age is not supported on sqlite")
        else:
            # Postgres + redshift
            return engine_support.postgres_age(fld)

    # Dates and datetimes

    def date(self, v):
        if isinstance(v, Tree):
            fld = self.column_lookup[v.data]
            if self.convert_dates_with:
                converter = getattr(self, self.convert_dates_with, None)
                if converter:
                    fld = converter(None, fld)
        else:
            fld = v
        return fld

    def date_fn(self, _, y, m, d):
        if self.drivername.startswith("mssql"):
            return func.datefromparts(y, m, d)
        else:
            return func.date(y, m, d)

    def datetime(self, v):
        if isinstance(v, Tree):
            fld = self.column_lookup[v.data]
            if self.convert_datetimes_with:
                converter = getattr(self, self.convert_datetimes_with, None)
                if converter:
                    fld = converter(None, fld)
        else:
            fld = v
        return fld

    def datetime_end(self, v):
        return self.column_lookup[v.data] if isinstance(v, Tree) else v

    def date_conv(self, _, datestr):
        try:
            dt = dateparser.parse(datestr)
        except Exception as e:
            raise e from e
        if dt:
            dt = dt.date()
        else:
            raise GrammarError(f"Can't convert '{datestr}' to a date.")
        return dt

    def datetime_conv(self, _, datestr):
        dt = dateparser.parse(datestr)
        if dt is None:
            raise GrammarError(f"Can't convert '{datestr}' to a datetime.")
        return dt

    def day_conv(self, _, fld):
        """Truncate to the day"""
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("day"))
        elif self.drivername.startswith("mssql"):
            return func.datefromparts(func.year(fld), func.month(fld), func.day(fld))
        else:
            # Postgres + redshift
            return func.date_trunc("day", fld)

    def week_conv(self, _, fld):
        """Truncate to mondays"""
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("week(monday)"))
        elif self.drivername.startswith("mssql"):
            raise GrammarError("week is not supported on mssql")
        else:
            # Postgres + redshift
            return func.date_trunc("week", fld)

    def month_conv(self, _, fld):
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("month"))
        elif self.drivername.startswith("mssql"):
            return func.datefromparts(func.year(fld), func.month(fld), literal_1)
        else:
            # Postgres + redshift
            return func.date_trunc("month", fld)

    def quarter_conv(self, _, fld):
        # Convert each date to the first day of each quarter
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("quarter"))
        elif self.drivername.startswith("mssql"):
            raise GrammarError("quarter is not supported on mssql")
        else:
            # Postgres + redshift
            return func.date_trunc("quarter", fld)

    def year_conv(self, _, fld):
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("year"))
        elif self.drivername.startswith("mssql"):
            return func.datefromparts(func.year(fld), literal_1, literal_1)
        else:
            # Postgres + redshift
            return func.date_trunc("year", fld)

    def dt_day_conv(self, _, fld):
        """Truncate to day"""
        if self.drivername == "bigquery":
            # Developer's note: The timestamp_trunc function in sqlalchemy-bigquery is not
            # type checked so the function return type is not annotated. This would
            # generate a type error when running the query like:
            # sqlalchemy.exc.DatabaseError: (google.cloud.bigquery.dbapi.exceptions.DatabaseError) 400
            # No matching signature for operator BETWEEN for argument types: TIMESTAMP, DATETIME, DATETIME
            #
            # We'll explicitly convert which removes timezone info.
            #
            # I tried to create a custom function in engine_support but this kept
            # creating a recursion error.
            return func.datetime(func.timestamp_trunc(fld, text("day")))
        elif self.drivername.startswith("mssql"):
            return func.datetimefromparts(
                func.year(fld),
                func.month(fld),
                func.day(fld),
                literal_0,
                literal_0,
                literal_0,
                literal_0,
            )
        else:
            # Postgres + redshift
            return func.date_trunc("day", fld)

    def dt_week_conv(self, _, fld):
        """Truncate to mondays"""
        if self.drivername == "bigquery":
            # See developer's note on dt_day_conv
            return func.datetime(func.timestamp_trunc(fld, text("week(monday)")))
        elif self.drivername.startswith("mssql"):
            raise GrammarError("week is not supported on mssql")
        else:
            # Postgres + redshift
            return func.date_trunc("week", fld)

    def dt_month_conv(self, _, fld):
        if self.drivername == "bigquery":
            # See developer's note on dt_day_conv
            return func.datetime(func.timestamp_trunc(fld, text("month")))
        elif self.drivername.startswith("mssql"):
            return func.datetimefromparts(
                func.year(fld),
                func.month(fld),
                literal_1,
                literal_0,
                literal_0,
                literal_0,
                literal_0,
            )
        else:
            # Postgres + redshift
            return func.date_trunc("month", fld)

    def dt_quarter_conv(self, _, fld):
        if self.drivername == "bigquery":
            # See developer's note on dt_day_conv
            return func.datetime(func.timestamp_trunc(fld, text("quarter")))
        elif self.drivername.startswith("mssql"):
            raise GrammarError("quarter is not supported on mssql")
        else:
            # Postgres + redshift
            return func.date_trunc("quarter", fld)

    def dt_year_conv(self, _, fld):
        if self.drivername == "bigquery":
            # See developer's note on dt_day_conv
            return func.datetime(func.datetime(func.timestamp_trunc(fld, text("year"))))
        elif self.drivername.startswith("mssql"):
            # SQL server can not support parameters in queries that are used for grouping
            # https://github.com/mkleehammer/pyodbc/issues/479
            # To avoid parameterization, we pass literals
            return func.datetimefromparts(
                func.year(fld),
                literal_1,
                literal_1,
                literal_0,
                literal_0,
                literal_0,
                literal_0,
            )
        else:
            # Postgres + redshift
            return func.date_trunc("year", fld)

    def datetime_end_conv(self, _, datestr):
        # Parse a datetime as the last moment of the given day
        # if the date
        dt = dateparser.parse(datestr)
        if dt is None:
            raise GrammarError(f"Can't convert '{datestr}' to a datetime.")
        return convert_to_eod_datetime(dt)

    def timedelta(self):
        pass

    # Booleans

    def and_boolean(self, left_boolean, AND, right_boolean):
        return and_(left_boolean, right_boolean)

    def paren_boolean(self, paren_boolean):
        return and_(paren_boolean)

    def or_boolean(self, left_boolean, OR, right_boolean):
        return or_(left_boolean, right_boolean)

    def not_boolean(self, NOT, boolean_expr):
        if boolean_expr in (True, False):
            return not boolean_expr
        else:
            return not_(boolean_expr)

    def consistent_array(self, *args):
        """A comma separated, variable length array of all numbers
        or all strings"""
        return args

    def vector_comparator(self, *args):
        """Can be one token "IN" or two "NOT IN" """
        return "in_" if len(args) == 1 else "notin_"

    def comparator(self, comp):
        """A comparator like =, !=, >, >="""
        comparators = {
            "=": "__eq__",
            ">": "__gt__",
            ">=": "__ge__",
            "!=": "__ne__",
            "<>": "__ne__",
            "<": "__lt__",
            "<=": "__le__",
            "IS": "__eq__",
            "ISNOT": "__ne__",
        }
        return comparators.get(str(comp).upper())

    def null_comparator(self, *args):
        comp = "="
        if len(args) == 1:
            comp = args[0]
        elif len(args) == 2:
            comp = "ISNOT"
        return self.comparator(comp)

    def between_expr(self, col, BETWEEN, left, AND, right):
        return between(col, left, right)

    def date_between_expr(self, col, BETWEEN, left, AND, right):
        """Auto convert strings to dates."""
        if isinstance(left, str):
            left = self.date_conv(None, left)
        if isinstance(right, str):
            right = self.date_conv(None, right)
        return self.between_expr(col, BETWEEN, left, AND, right)

    def datetime_between_expr(self, col, BETWEEN, left, AND, right):
        """Auto convert strings to datetimes."""
        if isinstance(left, str):
            left = self.datetime_conv(None, left)
        if isinstance(right, str):
            right = self.datetime_end_conv(None, right)
        return self.between_expr(col, BETWEEN, left, AND, right)

    def intelligent_date_expr(self, datecol, IS, offset, units):
        start, end = calc_date_range(offset, units, date.today())
        return between(datecol, start, end)

    def intelligent_datetime_expr(self, datetimecol, IS, offset, units):
        start, end = calc_date_range(offset, units, date.today())
        start = convert_to_start_datetime(start)
        end = convert_to_end_datetime(end)
        return between(datetimecol, start, end)

    def vector_expr(self, left, vector_comparator, num_or_str_array):
        if hasattr(left, vector_comparator):
            return getattr(left, vector_comparator)(num_or_str_array)
        else:
            self._raise_error("This value must be a column or column expression")

    def bool_expr(self, left, comparator, right):
        """A boolean expression like score > 20

        If left is a primitive, swap the order:
        20 > score => score < 20
        """
        # If the left is a primitive, try to swap the sides
        if isinstance(left, (str, int, float, bool, date, datetime)):
            swap_comp = {
                "__gt__": "__lt__",
                "__lt__": "__gt__",
                "__ge__": "__le__",
                "__le__": "__ge__",
            }
            comparator = swap_comp.get(comparator, comparator)
            left, right = right, left

        if right is None and comparator in ("__eq__", "__ne__"):
            is_comp = {"__eq__": "is_", "__ne__": "isnot"}
            comparator = is_comp.get(comparator, comparator)

        return getattr(left, comparator)(right)

    def date_bool_expr(self, left, comparator, right):
        """If right is still a string, convert to a date."""
        if isinstance(right, str):
            right = self.date_conv(None, right)
        return self.bool_expr(left, comparator, right)

    def datetime_bool_expr(self, left, comparator, right):
        """If right is still a string, convert to a date."""
        if isinstance(right, str):
            right = self.datetime_conv(None, right)
        return self.bool_expr(left, comparator, right)

    def str_like_expr(self, left, comparator, right):
        """If right doesn't contain a wildcard, search for right
        anywhere in the string."""
        if "_" not in right and "%" not in right:
            right = f"%{right}%"
        if comparator.lower() == "like":
            return left.like(right)
        elif comparator.lower() == "ilike":
            return left.ilike(right)
        else:
            raise Exception("Unknown comparator")

    # Aggregations

    def aggr(self, v):
        return v

    def date_aggr(self, v):
        return v

    def datetime_aggr(self, v):
        return v

    def string_aggr(self, v):
        return v

    def sum_aggr(self, _, fld):
        """Sum up the things"""
        return func.sum(fld)

    def min_aggr(self, _, fld):
        """Sum up the things"""
        return func.min(fld)

    def max_aggr(self, _, fld):
        """Sum up the things"""
        return func.max(fld)

    def avg_aggr(self, _, fld):
        """Sum up the things"""
        return func.avg(fld)

    def count_aggr(self, _, fld):
        """Sum up the things"""
        if getattr(fld, "data", None) == "star":
            return func.count()
        else:
            return func.count(fld)

    def percentile_aggr(self, percentile, fld):
        """Sum up the things"""
        percentile_val = int(percentile[len("percentile") :])
        if percentile_val not in (1, 5, 10, 25, 50, 75, 90, 95, 99):
            raise GrammarError(
                f"percentile values of {percentile_val} is not supported."
            )
        if self.drivername == "bigquery":
            percentile_fn = getattr(engine_support, f"bq_percentile{percentile_val}")
            return percentile_fn(fld)
        elif self.drivername == "sqlite":
            raise GrammarError("Percentile is not supported on sqlite")
        else:
            # Postgres + redshift
            return func.percentile_cont(percentile_val / 100.0).within_group(fld)

    def count_distinct_aggr(self, _, fld):
        """Sum up the things"""
        return func.count(distinct(fld))

    # If functions

    def if_statement(self, IF, *args):
        args = list(args)

        # If there's an odd number of args, pop the last one to use as the else
        if len(args) % 2:
            else_expr = args.pop()
        else:
            else_expr = None

        # collect the other args into pairs
        # ['a','b','c','d'] --> [('a',b'), ('c','d')]
        pairs = zip(args[::2], args[1::2])
        return case(pairs, else_=else_expr)

    # Constants

    def ESCAPED_STRING(self, v):
        v = str(v)
        if v.startswith('"') and v.endswith('"'):
            return v[1:-1]
        return v

    def NUMBER(self, v):
        try:
            n = int(v)
        except ValueError:
            n = float(v)
        return n

    def TRUE(self, v):
        return True

    def FALSE(self, v):
        return False

    def NULL(self, v):
        return None
