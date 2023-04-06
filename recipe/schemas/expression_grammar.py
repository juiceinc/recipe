import re
from datetime import date, datetime
from typing import List, Optional

import attr
import structlog
from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, alias, cast
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.sql.base import ColumnCollection
from sqlalchemy.sql.sqltypes import Numeric

SLOG = structlog.get_logger(__name__)


VALID_COLUMN_RE = re.compile("^\w+$")


def is_valid_column(colname: str) -> bool:
    """We can only build columns on field names that are alphanumeric"""
    return bool(VALID_COLUMN_RE.match(colname))


@attr.s
class Col:
    """Link a sqlalchemy column with a grammar rule"""

    datatype: str = attr.ib()
    sqla_col = attr.ib()
    name: str = attr.ib()
    idx: int = attr.ib(default=-1)
    namespace: str = attr.ib(default="")

    @classmethod
    def make_from_sqla_col(cls, sqla_col):
        if is_valid_column(sqla_col.name):
            if isinstance(sqla_col.type, String):
                datatype = "str"
            elif isinstance(sqla_col.type, Date):
                datatype = "date"
            elif isinstance(sqla_col.type, DateTime):
                datatype = "datetime"
            elif isinstance(sqla_col.type, Integer):
                datatype = "num"
            elif isinstance(sqla_col.type, Numeric):
                datatype = "num"
            elif isinstance(sqla_col.type, Boolean):
                datatype = "bool"
            else:
                datatype = "unusable"
            return cls(
                namespace="", datatype=datatype, sqla_col=sqla_col, name=sqla_col.name
            )

    @classmethod
    def make_from_constant(cls, key, value):
        """Make a column from a scalar constant"""
        if is_valid_column(key):
            datatype = "unusable"
            sqla_col = None

            if isinstance(value, str):
                datatype = "str"
                sqla_col = cast(value, String)
            elif isinstance(value, date):
                datatype = "date"
                sqla_col = cast(value, Date)
            elif isinstance(value, datetime):
                datatype = "datetime"
                sqla_col = cast(value, DateTime)
            elif isinstance(value, int):
                datatype = "num"
                sqla_col = cast(value, Integer)
            elif isinstance(value, float):
                datatype = "num"
                sqla_col = cast(value, Float)
            elif isinstance(value, bool):
                datatype = "bool"
                sqla_col = cast(value, Boolean)
            return cls(namespace="", datatype=datatype, sqla_col=sqla_col, name=key)

    @property
    def rule_name(self) -> str:  # sourcery skip: raise-specific-error
        """The name for the lark rule for this column"""
        if self.idx == -1:
            raise Exception("Must assign indexes first")
        return f"{self.datatype}_{self.idx}"

    @property
    def field_name(self) -> str:
        """What to call this column in expressions."""
        return f"{self.namespace}\\.{self.name}" if self.namespace else self.name

    def as_rule(self):
        return f'    {self.rule_name}: "[" + /{self.field_name}/i + "]" | /{self.field_name}/i'


@attr.s
class ColCollection:
    """A collection of columns. These columns may come from more than one selectable."""

    columns: List[Col] = attr.ib()

    def assign_indexes(self):
        """Sort columns by datatype and assign an index"""
        idx = 0
        prev_datatype = None
        for col in sorted(self.columns, key=lambda c: (c.datatype, c.name)):
            if col.datatype != prev_datatype:
                idx = 0
            col.idx = idx
            prev_datatype = col.datatype
            idx += 1

    def set_namespace(self, namespace):
        for c in self.columns:
            c.namespace = namespace

    def extend(self, other_cc):
        self.columns += other_cc.columns

    def column_lookup(self) -> dict:
        """Generate a lookup from rule names to the sqlalchemy columns"""
        return {c.rule_name: c.sqla_col for c in self.columns}


def make_column_collection_for_selectable(
    selectable, *, namespace: Optional[str] = None
) -> ColCollection:
    """Return a dictionary of columns. The keys
    are unique lark rule names prefixed by the column type
    like num_0, num_1, string_0, etc.

    The values are the selectable column reference
    """
    from recipe import Recipe

    if isinstance(selectable, Recipe):
        selectable = selectable.subquery()

    if isinstance(selectable, DeclarativeMeta):
        column_iterable = selectable.__table__.columns
    # Selectable is a sqlalchemy subquery
    elif hasattr(selectable, "c") and isinstance(selectable.c, ColumnCollection):
        column_iterable = selectable.c
    else:
        raise Exception("Selectable does not have columns")

    columns = []
    for sqla_col in column_iterable:
        if col := Col.make_from_sqla_col(sqla_col):
            columns.append(col)

    cc = ColCollection(columns)
    if namespace:
        cc.set_namespace(namespace=namespace)
    return cc


def is_constant_expression(v) -> bool:
    return isinstance(v, str) and "(" in v and ")" in v


def has_constant_expressions(constants: dict) -> bool:
    return any(is_constant_expression(v) for v in constants.values())


def has_constant_literals(constants: dict) -> bool:
    return any(not is_constant_expression(v) for v in constants.values())


def make_column_collection_for_constant_literals(
    constants: dict, *, namespace: Optional[str] = None
) -> ColCollection:
    """
    Constants are a dict of names to scalar values to use in expressions
    We will treat them as if they are another column collection

    Args:
        selectable: A selectable for column expressions
        constants (dict): A dict with string keys and scalar values
        namespace (str, optional): A namespace to add. Defaults to None.

    Returns:
        ColumnCollection: A column collection of constant values
    """
    # Create columns
    constant_columns = [
        Col.make_from_constant(k, v)
        for k, v in constants.items()
        if not is_constant_expression(v)
    ]
    cc = ColCollection(constant_columns)
    if namespace:
        cc.set_namespace(namespace=namespace)
    return cc


def make_column_collection_for_constant_expressions(
    builder, constants: dict, *, namespace: Optional[str] = None
) -> ColCollection:
    """
    Constants are a dict of names to scalar values to use in expressions
    We will treat them as if they are another column collection

    Args:
        builder: A selectable for column expressions
        constants (dict): A dict with string keys and scalar values
        namespace (str, optional): A namespace to add. Defaults to None.

    Returns:
        ColumnCollection: A column collection of constant values
    """
    # Create columns
    from sqlalchemy import select

    expression_columns = []
    sel = select()
    for k, v in constants.items():
        if is_constant_expression(v):
            expr, dtype = builder.parse(v)
            expression_columns.append(expr.label(k))

    sel = alias(select(expression_columns), "constants")
    return make_column_collection_for_selectable(sel, namespace=namespace)


def make_columns_grammar(cc: ColumnCollection) -> str:
    """Return a lark rule that looks like

    // These are my raw columns
    str_0: "[" + /username/i + "]" | /username/i
    str_1: "[" + /department/i + "]" | /department/i
    str_2: "[" + /testid/i + "]" | /testid/i
    num_0: "[" + /score/i + "]" | /score/i
    """
    cc.assign_indexes()
    return "\n".join(sorted([col.as_rule() for col in cc.columns]))


def gather_columns(
    datatype_rule_name: str,
    cc: ColumnCollection,
    datatype: str,
    *,
    additional_rules=None,
) -> str:
    """Build a list of all column rules matching a datatype along with potential additional rules."""
    if additional_rules is None:
        additional_rules = []

    matching_cols = [c for c in cc.columns if c.datatype == datatype]
    column_rules = [f"{datatype}_{n}" for n in range(len(matching_cols))]
    if matching_cols + additional_rules:
        raw_rule_name = datatype_rule_name.split(".")[0]

        # Reduce a pair of parens around a type back to itself.
        paren_rule = f'"(" + {raw_rule_name} + ")"'

        return f"{datatype_rule_name}: " + " | ".join(
            column_rules + additional_rules + [paren_rule]
        )
    else:
        return f'{datatype_rule_name}: "DUMMYVALUNUSABLECOL"'


def make_grammar(columns):
    """Build a grammar for this selectable using columns"""
    grammar = f"""
    col: boolean | string | num | date | datetime_end | datetime | unusable_col | unknown_col | error_math | error_vector_expr | error_not_nonboolean | error_between_expr | error_aggr | error_if_statement
    //paren_col: "(" col ")" -> col

    // These are the raw columns in the selectable
    {make_columns_grammar(columns)}

    {gather_columns("unusable_col", columns, "unusable")}
    {gather_columns("date.1", columns, "date", additional_rules=["date_conv", "date_fn", "day_conv", "week_conv", "month_conv", "quarter_conv", "year_conv", "dt_day_conv", "dt_week_conv", "dt_month_conv", "dt_quarter_conv", "dt_year_conv", "datetime_to_date_conv", "date_aggr", "date_if_statement", "date_coalesce"])}
    {gather_columns("datetime.2", columns, "datetime", additional_rules=["datetime_conv", "datetime_if_statement", "datetime_coalesce"])}
    // Datetimes that are converted to the end of day
    {gather_columns("datetime_end.1", columns, "datetime", additional_rules=["datetime_end_conv", "datetime_aggr"])}
    {gather_columns("boolean.1", columns, "bool", additional_rules=["TRUE", "FALSE", "bool_expr", "date_bool_expr", "datetime_bool_expr", "str_like_expr", "vector_expr", "between_expr", "date_between_expr", "datetime_between_expr", "not_boolean", "or_boolean", "and_boolean", "paren_boolean", "intelligent_date_expr", "intelligent_datetime_expr"])}
    {gather_columns("string.1", columns, "str", additional_rules=["ESCAPED_STRING", "string_add", "string_cast", "string_coalesce", "string_substr", "string_if_statement", "string_aggr"])}
    {gather_columns("num.1", columns, "num", additional_rules=["NUMBER", "num_add", "num_sub", "num_mul", "num_div", "int_cast", "num_coalesce", "aggr", "error_aggr", "num_if_statement", "age_conv"])}
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
    str_like_expr: string LIKE ESCAPED_STRING
    date_bool_expr.1: date comparator (date | string)
    datetime_bool_expr.2: datetime comparator (datetime | string)
    comparator: EQ | NE | LT | LTE | GT | GTE
    null_comparator: EQ | NE | IS | IS NOT
    LIKE: /i?like/i
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
    string_substr: /substr/i "(" string "," [num ("," num)?] ")"
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
    count_aggr.1: /count/i "(" (num | string | date | datetime | boolean | star) ")"
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
