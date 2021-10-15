from collections import defaultdict
from datetime import date, datetime

import dateparser
import functools
from lark import GrammarError, Lark, Transformer, Tree, Visitor, v_args
from lark.lexer import Token
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
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
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.sql.sqltypes import Numeric

from . import engine_support
from .utils import (
    calc_date_range,
    convert_to_end_datetime,
    convert_to_eod_datetime,
    convert_to_start_datetime,
)


def make_columns_grammar(columns):
    """Return a lark rule that looks like

    // These are my raw columns
    str_0: "[" + /username/i + "]" | /username/i
    str_1: "[" + /department/i + "]" | /department/i
    str_2: "[" + /testid/i + "]" | /testid/i
    """
    items = []
    for k in sorted(columns.keys()):
        c = columns[k]
        items.append(f'    {k}: "[" + /{c.name}/i + "]" | /{c.name}/i')
    return "\n".join(items).lstrip()


def gather_columns(rule_name, columns, prefix, additions=None):
    """Build a list of all columns matching a prefix allong with potential additional rules."""
    raw_rule_name = rule_name.split(".")[0]
    if additions is None:
        additions = []

    # Reduce a pair of parens around a type back to itself.
    paren_rule = f'"(" + {raw_rule_name} + ")"'

    matching_keys = [k for k in sorted(columns.keys()) if k.startswith(prefix + "_")]
    if matching_keys + additions:
        return f"{rule_name}: " + " | ".join(matching_keys + additions + [paren_rule])
    else:
        return f'{rule_name}: "DUMMYVALUNUSABLECOL"'


def make_columns_for_table(selectable):
    """Return a dictionary of columns. The keys
    are unique lark rules prefixed by the column type
    like num_0, num_1, string_0, etc.

    The values are the selectable column reference
    """
    from recipe import Recipe

    if isinstance(selectable, Recipe):
        selectable = selectable.subquery()

    if isinstance(selectable, DeclarativeMeta):
        column_iterable = selectable.__table__.columns
    # Selectable is a sqlalchemy subquery
    elif hasattr(selectable, "c") and isinstance(
        selectable.c, ImmutableColumnCollection
    ):
        column_iterable = selectable.c
    else:
        raise Exception("Selectable does not have columns")

    columns = {}
    type_counter = defaultdict(int)

    for c in column_iterable:
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
        cnt = type_counter[prefix]
        type_counter[prefix] += 1
        columns[f"{prefix}_{cnt}"] = c
    return columns


def make_lark_grammar(columns):
    """Build a grammar for this selectable using columns"""
    grammar = f"""
    col: boolean | string | num | date | datetime_end | datetime | unusable_col | unknown_col | error_math | error_vector_expr | error_not_nonboolean | error_between_expr | error_aggr | error_if_statement
    //paren_col: "(" col ")" -> col

    // These are the raw columns in the selectable
    {make_columns_grammar(columns)}

    {gather_columns("unusable_col", columns, "unusable", [])}
    {gather_columns("date.1", columns, "date", ["date_conv", "date_fn", "day_conv", "week_conv", "month_conv", "quarter_conv", "year_conv", "dt_day_conv", "dt_week_conv", "dt_month_conv", "dt_quarter_conv", "dt_year_conv", "datetime_to_date_conv", "date_aggr", "date_if_statement", "date_coalesce"])}
    {gather_columns("datetime.2", columns, "datetime", ["datetime_conv", "datetime_if_statement", "datetime_coalesce"])}
    // Datetimes that are converted to the end of day
    {gather_columns("datetime_end.1", columns, "datetime", ["datetime_end_conv", "datetime_aggr"])}
    {gather_columns("boolean.1", columns, "bool", ["TRUE", "FALSE", "bool_expr", "date_bool_expr", "datetime_bool_expr", "vector_expr", "between_expr", "date_between_expr", "datetime_between_expr", "not_boolean", "or_boolean", "and_boolean", "paren_boolean", "intelligent_date_expr", "intelligent_datetime_expr"])}
    {gather_columns("string.1", columns, "str", ["ESCAPED_STRING", "string_add", "string_cast", "string_coalesce", "string_if_statement", "string_aggr"])}
    {gather_columns("num.1", columns, "num", ["NUMBER", "num_add", "num_sub", "num_mul", "num_div", "int_cast", "num_coalesce", "aggr", "error_aggr", "num_if_statement", "age_conv"])}
    string_add: string "+" string
    num_add.1: num "+" num | "(" num "+" num ")"
    num_sub.1: num "-" num | "(" num "-" num ")"
    num_mul.2: num "*" num | "(" num "*" num ")"
    num_div.2: num "/" num | "(" num "/" num ")"
    add: col "+" col | "(" col "+" col ")"

    // Various error conditions (fields we don't recognize, bad math)
    // Low priority matching of any [columnname] values
    unknown_col.0: "[" + NAME + "]" | NAME
    error_math.0: error_add | error_sub | error_mul | error_div
    error_add.0: col "+" col
    error_sub.0: col "-" col
    error_mul.0: col "*" col
    error_div.0: col "/" col
    error_between_expr.0: col BETWEEN col AND col
    error_vector_expr.0: col vector_comparator mixedarray
    error_not_nonboolean: NOT string | NOT num
    mixedarray.0: "(" [CONSTANT ("," CONSTANT)*] ","? ")"
    CONSTANT: ESCAPED_STRING | NUMBER

    // Boolean scalar expressions like 'a > b'
    paren_boolean.5: "(" boolean ")"
    not_boolean.4: NOT boolean
    and_boolean.3: boolean AND boolean
    or_boolean.2: boolean OR boolean
    bool_expr: col comparator col | col null_comparator NULL
    date_bool_expr.1: date comparator (date | string)
    datetime_bool_expr.2: datetime comparator (datetime | string)
    comparator: EQ | NE | LT | LTE | GT | GTE
    null_comparator: EQ | NE | IS | IS NOT
    EQ: "="
    NE: "!=" | "<>"
    LT: "<"
    LTE: "<="
    GT: ">"
    GTE: ">="

    // Boolean vector expressions like 'a in (array of constants)'
    intelligent_date_expr.1: date IS INTELLIGENT_DATE_OFFSET INTELLIGENT_DATE_UNITS
    intelligent_datetime_expr.1: datetime IS INTELLIGENT_DATE_OFFSET INTELLIGENT_DATE_UNITS
    between_expr.1: string BETWEEN string AND string | num BETWEEN num AND num | date BETWEEN date AND date | datetime BETWEEN datetime AND datetime_end
    date_between_expr.2: date BETWEEN (date | string) AND (date | string)
    datetime_between_expr.3: datetime BETWEEN (datetime | string) AND (datetime_end | string)

    vector_expr.1: string vector_comparator stringarray | num vector_comparator numarray
    vector_comparator.1: NOT? IN
    stringarray.1: "(" [ESCAPED_STRING ("," ESCAPED_STRING)*] ","? ")"  -> consistent_array
    numarray.1: "(" [NUMBER ("," NUMBER)*] ","?  ")"                    -> consistent_array

    // Date
    date_conv.3: /date/i "(" ESCAPED_STRING ")"
    date_fn.3: /date/i "(" num "," num "," num ")"
    datetime_to_date_conv.3: /date/i "(" datetime ")"  -> dt_day_conv
    datetime_conv.2: /date/i "(" ESCAPED_STRING ")"
    datetime_end_conv.1: /date/i "(" ESCAPED_STRING ")"

    // Conversions
    // date->date
    day_conv: /day/i "(" date ")"
    week_conv: /week/i "(" date ")"
    month_conv: /month/i "(" date ")"
    quarter_conv: /quarter/i "(" date ")"
    year_conv: /year/i "(" date ")"
    // datetime->date
    dt_day_conv: /day/i "(" datetime ")"
    dt_week_conv: /week/i "(" datetime ")"
    dt_month_conv: /month/i "(" datetime ")"
    dt_quarter_conv: /quarter/i "(" datetime ")"
    dt_year_conv: /year/i "(" datetime ")"
    // col->string
    string_cast: /string/i "(" col ")"
    // col->int
    int_cast: /int/i "(" col ")"
    // date->int
    age_conv: /age/i "(" (date | datetime) ")"
    // date->int
    // TODO: age_conv: /age/i "(" (date | datetime) ")"
    // TODO: date - date => int
    num_coalesce: /coalesce/i "(" num "," num ")"                 -> coalesce
    string_coalesce: /coalesce/i "(" string "," string ")"        -> coalesce
    date_coalesce: /coalesce/i "(" date "," date ")"              -> coalesce
    datetime_coalesce: /coalesce/i "(" datetime "," datetime ")"  -> coalesce

    // Aggregations that are errors
    error_aggr.0: error_sum_aggr | error_min_aggr | error_max_aggr | error_avg_aggr | error_median_aggr | error_percentile_aggr
    error_sum_aggr.0: /sum/i "(" col ")"
    error_min_aggr.0: /min/i "(" col ")"
    error_max_aggr.0: /max/i "(" col ")"
    error_avg_aggr.0: /avg/i "(" col ")" | /average/i "(" col ")"
    error_median_aggr.0: /median/i "(" col ")"
    error_percentile_aggr.0: /percentile\d\d?/i "(" col ")"
    // Aggregations that return numbers
    aggr.1: sum_aggr | min_aggr | max_aggr | avg_aggr | count_aggr | count_distinct_aggr | median_aggr | percentile_aggr
    sum_aggr.1: /sum/i "(" num ")"
    min_aggr.1: /min/i "(" num ")"
    max_aggr.1: /max/i "(" num ")"
    avg_aggr.1: /avg/i "(" num ")" | /average/i "(" num ")"
    count_aggr.1: /count/i "(" (num | string | date | datetime | star) ")"
    count_distinct_aggr.1: /count_distinct/i "(" (num | string | date | datetime | boolean) ")"
    median_aggr.1: /median/i "(" num ")"
    percentile_aggr.1: /percentile\d\d?/i "(" num ")"
    // Aggregations that return strings
    string_aggr.1: min_string_aggr | max_string_aggr
    min_string_aggr.1: /min/i "(" string ")"            -> min_aggr
    max_string_aggr.1: /max/i "(" string ")"            -> max_aggr
    // Aggregations that return dates
    date_aggr.1: min_date_aggr | max_date_aggr
    min_date_aggr.1: /min/i "(" date ")"            -> min_aggr
    max_date_aggr.1: /max/i "(" date ")"            -> max_aggr
    // Aggregations that return datetimes
    datetime_aggr.1: min_datetime_aggr | max_datetime_aggr
    min_datetime_aggr.1: /min/i "(" datetime ")"    -> min_aggr
    max_datetime_aggr.1: /max/i "(" datetime ")"    -> max_aggr

    // functions
    num_if_statement.4: IF "(" (boolean "," (num | NULL) ","?)+ (num | NULL)? ")"                    -> if_statement
    string_if_statement.4: IF "(" (boolean "," (string | NULL) ","?)+ (string | NULL)? ")"           -> if_statement
    date_if_statement.4: IF "(" (boolean "," (date | NULL) ","?)+ (date | NULL)? ")"                 -> if_statement
    datetime_if_statement.4: IF "(" (boolean "," (datetime | NULL) ","?)+ (datetime | NULL)? ")"     -> if_statement
    //error_if_statement.3: IF "(" (col "," (col | NULL) ","?)+ (col | NULL)? ")"
    error_if_statement.3: IF "(" (col "," (col | NULL) ","?)+ (col | NULL)? ")"

    star: "*"
    TRUE: /TRUE/i
    FALSE: /FALSE/i
    OR: /OR/i
    AND: /AND/i
    NOT: /NOT/i
    IN: /IN/i
    IS: /IS/i
    BETWEEN: /BETWEEN/i
    NULL: /NULL/i
    IF: /IF/i
    INTELLIGENT_DATE_OFFSET: /prior/i | /last/i | /previous/i | /current/i | /this/i | /next/i
    INTELLIGENT_DATE_UNITS: /ytd/i | /year/i | /qtr/i | /month/i | /mtd/i | /day/i
    COMMENT: /#.*/

    %import common.CNAME                       -> NAME
    %import common.SIGNED_NUMBER               -> NUMBER
    %import common.ESCAPED_STRING
    %import common.WS_INLINE
    %ignore COMMENT
    %ignore WS_INLINE
"""
    return grammar


class SQLALchemyValidator(Visitor):
    def __init__(self, text, forbid_aggregation, drivername):
        """Visit the tree and return descriptive information. Populate
        a list of errors.

        Args:
            text (str): A copy of the parsed text for error descriptions
            forbid_aggregation (bool): Should aggregations be treated as an error
            drivername (str): The database engine we are running against
        """
        self.text = text
        self.forbid_aggregation = forbid_aggregation
        self.drivername = drivername

        # Was an aggregation encountered in the tree?
        self.found_aggregation = False
        # What is the datatype of the returned expression
        self.last_datatype = None
        # Errors encountered while visiting the tree
        self.errors = []

    def _data_type(self, tree):
        # Find the data type for a tree
        if tree is None:
            return None
        if tree.data == "col":
            dt = self._data_type(tree.children[0])
        else:
            dt = tree.data
        if dt == "datetime_end":
            dt = "datetime"
        elif dt == "string":
            dt = "str"
        elif dt == "boolean":
            dt = "bool"
        return dt

    def _add_error(self, message, tree):
        """Add an error pointing to this location in the parsed string"""
        tok = None
        # Find the first token
        while tree and tree.children:
            tree = tree.children[0]
            if isinstance(tree, Token):
                tok = tree
                break

        if tok:
            extra_context = self._get_context_for_token(tok, span=200)
            message = f"{message}\n\n{extra_context}"
        self.errors.append(message)

    def _get_context_for_token(self, tok, span=40):
        pos = tok.pos_in_stream
        start = max(pos - span, 0)
        end = pos + span
        before = self.text[start:pos].rsplit("\n", 1)[-1]
        after = self.text[pos:end].split("\n", 1)[0]
        return before + after + "\n" + " " * len(before) + "^\n"

    def _error_math(self, tree, verb):
        tok1 = tree.children[0].children[0]
        tok2 = tree.children[1].children[0]
        self._add_error(f"{tok1.data} and {tok2.data} can not be {verb}", tree)

    def col(self, tree):
        self.last_datatype = self._data_type(tree)

    def error_add(self, tree):
        self._error_math(tree, "added together")

    def error_mul(self, tree):
        self._error_math(tree, "multiplied together")

    def error_sub(self, tree):
        self._error_math(tree, "subtracted")

    def error_div(self, tree):
        self._error_math(tree, "divided")

    def error_if_statement(self, tree):
        args = tree.children
        # Throw away the "if"
        args = args[1:]

        # If there's an odd number of args, pop the last one to use as the else
        if len(args) % 2:
            else_expr = args.pop()
        else:
            else_expr = None

        # The "odd" args should be booleans
        bool_args = args[::2]
        # The "even" args should be values of the same type
        value_args = args[1::2]

        # Check that the boolean args are boolean
        for arg in bool_args:
            dt = self._data_type(arg)
            if dt != "bool":
                self._add_error("This should be a boolean column or expression", arg)

        # Data types of columns must match
        value_type = None
        for arg in value_args + [else_expr]:
            dt = self._data_type(arg)
            if dt is not None:
                if value_type is None:
                    value_type = dt
                elif value_type != dt:
                    self._add_error(
                        f"The values in this if statement must be the same type, not {value_type} and {dt}",
                        arg,
                    )

    def aggr(self, tree):
        self.found_aggregation = True
        if self.forbid_aggregation:
            self._add_error(f"Aggregations are not allowed in this field.", tree)

    def unknown_col(self, tree):
        """Column name doesn't exist in the data"""
        tok1 = tree.children[0]
        self._add_error(f"{tok1} is not a valid column name", tree)

    def unusable_col(self, tree):
        """Column name isn't a data type we can handle"""
        tok1 = tree.children[0]
        self._add_error(
            f"{tok1} is a data type that can't be used. Usable data types are strings, numbers, boolean, dates, and datetimes",
            tree,
        )

    def error_not_nonboolean(self, tree):
        """NOT string or NOT num"""
        self._add_error(f"NOT requires a boolean value", tree)

    def mixedarray(self, tree):
        """An array containing a mix of strings and numbers"""
        self._add_error(f"An array may not contain both strings and numbers", tree)

    def vector_expr(self, tree):
        val, comp, arr = tree.children
        # If the left hand side is a number or string primitive
        if isinstance(val.children[0], Token) and val.children[0].type in (
            "NUMBER",
            "ESCAPED_STRING",
        ):
            self._add_error(f"Must be a column or expression", val)

    def error_aggr(self, tree):
        """Aggregating a bad data type"""
        fn = tree.children[0].children[0]
        dt = self._data_type(tree.children[0].children[1])
        self._add_error(f"A {dt} can not be aggregated using {fn}.", tree)

    def error_between_expr(self, tree):
        col, BETWEEN, left, AND, right = tree.children
        col_type = self._data_type(col)
        left_type = self._data_type(left)
        right_type = self._data_type(right)
        if col_type == "datetime":
            if left_type == "date":
                left_type = "datetime"
            if right_type == "date":
                right_type = "datetime"
        if not (col_type == left_type == right_type):
            self._add_error(
                f"When using between, the column ({col_type}) and between values ({left_type}, {right_type}) must be the same data type.",
                tree,
            )

    def bool_expr(self, tree):
        """a > b where the types of a and b don't match"""
        left, _, right = tree.children
        if isinstance(left, Tree) and isinstance(right, Tree):
            left_data_type = self._data_type(left)
            right_data_type = self._data_type(right)
            if left_data_type == right_data_type == "date":
                return
            if left_data_type == right_data_type == "datetime":
                return
            if left_data_type in ("date", "datetime") and right_data_type == "string":
                # Strings will be auto converted
                return
            if left_data_type != right_data_type:
                self._add_error(
                    f"Can't compare {left_data_type} to {right_data_type}", tree
                )

    def percentile_aggr(self, tree):
        """Sum up the things"""
        percentile, fld = tree.children
        percentile_val = int(percentile[len("percentile") :])
        if percentile_val not in (1, 5, 10, 25, 50, 75, 90, 95, 99):
            self._add_error(
                f"Percentile values of {percentile_val} are not supported.", tree
            )
        if self.drivername == "sqlite":
            self._add_error("Percentile is not supported on sqlite", tree)


@v_args(inline=True)  # Affects the signatures of the methods
class TransformToSQLAlchemyExpression(Transformer):
    """Converts a field to a SQLAlchemy expression"""

    def __init__(
        self,
        selectable,
        columns,
        drivername,
        forbid_aggregation=True,
    ):
        self.text = None
        self.selectable = selectable
        self.columns = columns
        self.last_datatype = None
        # Convert all dates with this conversion
        self.convert_dates_with = None
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
        pos = tok.pos_in_stream
        start = max(pos - span, 0)
        end = pos + span
        before = self.text[start:pos].rsplit("\n", 1)[-1]
        after = self.text[pos:end].split("\n", 1)[0]
        return before + after + "\n" + " " * len(before) + "^\n"

    def col(self, v):
        return v

    def string(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def string_cast(self, _, fld):
        """Cast a field to a string"""
        return cast(fld, String())

    def num(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def int_cast(self, _, fld):
        """Cast a field to a string"""
        return cast(fld, Integer())

    def coalesce(self, coalesce, left, right):
        """Coalesce a number, string, date or datetime"""
        return func.coalesce(left, right)

    def boolean(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def num_add(self, a, b):
        """Add numbers or strings"""
        return a + b

    def string_add(self, a, b):
        """Add numbers or strings"""
        return a + b

    def num_div(self, num, denom):
        """SQL safe division"""
        if isinstance(denom, (int, float)):
            if denom == 0:
                raise GrammarError("When dividing, the denominator can not be zero")
            elif denom == 1:
                return num
            elif isinstance(num, (int, float)):
                return num / denom
            else:
                return cast(num, Float) / denom
        else:
            if isinstance(num, (int, float)):
                return case([(denom == 0, None)], else_=num / cast(denom, Float))
            else:
                return case(
                    [(denom == 0, None)], else_=cast(num, Float) / cast(denom, Float)
                )

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
            fld = self.columns[v.data]
            if self.convert_dates_with:
                converter = getattr(self, self.convert_dates_with, None)
                if converter:
                    fld = converter(None, fld)
        else:
            fld = v
        return fld

    def date_fn(self, _, y, m, d):
        return func.date(y, m, d)

    def datetime(self, v):
        if isinstance(v, Tree):
            fld = self.columns[v.data]
            if self.convert_datetimes_with:
                converter = getattr(self, self.convert_datetimes_with, None)
                if converter:
                    fld = converter(None, fld)
        else:
            fld = v
        return fld

    def datetime_end(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def date_conv(self, _, datestr):
        dt = dateparser.parse(datestr)
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
        else:
            # Postgres + redshift
            return func.date_trunc("day", fld)

    def week_conv(self, _, fld):
        """Truncate to mondays"""
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("week(monday)"))
        else:
            # Postgres + redshift
            return func.date_trunc("week", fld)

    def month_conv(self, _, fld):
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("month"))
        else:
            # Postgres + redshift
            return func.date_trunc("month", fld)

    def quarter_conv(self, _, fld):
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("quarter"))
        else:
            # Postgres + redshift
            return func.date_trunc("quarter", fld)

    def year_conv(self, _, fld):
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("year"))
        else:
            # Postgres + redshift
            return func.date_trunc("year", fld)

    def dt_day_conv(self, _, fld):
        """Truncate to day"""
        if self.drivername == "bigquery":
            return func.timestamp_trunc(fld, text("day"))
        else:
            # Postgres + redshift
            return func.date_trunc("day", fld)

    def dt_week_conv(self, _, fld):
        """Truncate to mondays"""
        if self.drivername == "bigquery":
            return func.timestamp_trunc(fld, text("week(monday)"))
        else:
            # Postgres + redshift
            return func.date_trunc("week", fld)

    def dt_month_conv(self, _, fld):
        if self.drivername == "bigquery":
            return func.timestamp_trunc(fld, text("month"))
        else:
            # Postgres + redshift
            return func.date_trunc("month", fld)

    def dt_quarter_conv(self, _, fld):
        if self.drivername == "bigquery":
            return func.timestamp_trunc(fld, text("quarter"))
        else:
            # Postgres + redshift
            return func.date_trunc("quarter", fld)

    def dt_year_conv(self, _, fld):
        if self.drivername == "bigquery":
            return func.timestamp_trunc(fld, text("year"))
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
        if len(args) == 1:
            return "in_"
        else:
            return "notin_"

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


BUILDER_CACHE = {}


class SQLAlchemyBuilder(object):
    @classmethod
    def get_builder(cls, selectable):
        if selectable not in BUILDER_CACHE:
            BUILDER_CACHE[selectable] = cls(selectable)
        return BUILDER_CACHE[selectable]

    @classmethod
    def clear_builder_cache(cls):
        global BUILDER_CACHE
        BUILDER_CACHE = {}

    def __init__(self, selectable):
        """Parse a recipe field by building a custom grammar that
        uses the colums in a selectable.

        Args:
            selectable (Table): A SQLAlchemy selectable
        """
        self.selectable = selectable
        # Database driver
        try:
            self.drivername = selectable.metadata.bind.url.drivername
        except Exception:
            self.drivername = "unknown"

        self.columns = make_columns_for_table(selectable)
        self.grammar = make_lark_grammar(self.columns)
        self.parser = Lark(
            self.grammar,
            parser="earley",
            ambiguity="resolve",
            start="col",
            propagate_positions=True,
            # predict_all=True,
        )
        self.transformer = TransformToSQLAlchemyExpression(
            self.selectable,
            self.columns,
            self.drivername,
        )

        # The data type of the last parsed expression
        self.last_datatype = None

    @functools.lru_cache(maxsize=None)
    def parse(
        self,
        text,
        forbid_aggregation=False,
        enforce_aggregation=False,
        debug=False,
        convert_dates_with=None,
        convert_datetimes_with=None,
    ):
        """Return a parse tree for text

        Args:
            text (str): A field expression
            forbid_aggregation (bool, optional):
              The expression may not contain aggregations. Defaults to False.
            enforce_aggregation (bool, optional):
              Wrap the expression in an aggregation if one is not provided. Defaults to False.
            debug (bool, optional): Show some debug info. Defaults to False.
            convert_dates_with (str, optional): A converter to use for date fields
            convert_datetimes_with (str, optional): A converter to use for datetime fields

        Raises:
            GrammarError: A description of any errors and where they occur

        Returns:
            A tuple of
                ColumnElement: A SQLALchemy expression
                DataType: The datatype of the expression (bool, date, datetime, num, str)
        """
        tree = self.parser.parse(text, start="col")
        validator = SQLALchemyValidator(text, forbid_aggregation, self.drivername)
        validator.visit(tree)
        self.last_datatype = validator.last_datatype

        if validator.errors:
            if debug:
                print("".join(validator.errors))
                print("Tree:\n" + tree.pretty())
            raise GrammarError("".join(validator.errors))
        else:
            if debug:
                print("Tree:\n" + tree.pretty())
            self.transformer.text = text
            self.transformer.convert_dates_with = convert_dates_with
            self.transformer.convert_datetimes_with = convert_datetimes_with
            expr = self.transformer.transform(tree)
            if (
                enforce_aggregation
                and not validator.found_aggregation
                and self.last_datatype == "num"
            ):
                return (func.sum(expr), self.last_datatype)
            else:
                return (expr, self.last_datatype)
