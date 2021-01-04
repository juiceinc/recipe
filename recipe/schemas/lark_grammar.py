from operator import truediv
from sqlalchemy.sql.sqltypes import Numeric
import dateparser
import dateparser
from dateutil.relativedelta import relativedelta
from datetime import datetime, date
from lark import Lark, Transformer, Visitor, v_args, GrammarError, Tree
from lark.lexer import Token
from lark.visitors import inline_args
from sqlalchemy import (
    String,
    Float,
    Date,
    DateTime,
    Boolean,
    Integer,
    between,
    case,
    cast,
    distinct,
    inspection,
    not_,
    and_,
    or_,
    func,
    text,
)
from .utils import (
    calc_date_range,
    convert_to_eod_datetime,
    convert_to_end_datetime,
    convert_to_start_datetime,
)

from collections import defaultdict


def convert(lst):
    return ["\[" + v + "\]" for v in lst]


def make_columns(columns):
    """Return a lark string that looks like

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
    """Build a list of all columns matching a prefix allong with potential addition """
    if additions is None:
        additions = []
    matching_keys = [k for k in sorted(columns.keys()) if k.startswith(prefix + "_")]
    if matching_keys + additions:
        return f"{rule_name}: " + " | ".join(matching_keys + additions)
    else:
        return f'{rule_name}: "DUMMYVALUNUSABLECOL"'


def make_grammar_for_table(selectable):
    """Build a dict of usable columns and a grammar for this selectable """
    columns = {}
    type_counter = defaultdict(int)

    for c in selectable.columns:
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

    grammar = f"""
    col: boolean | string | num | date | datetime_end | datetime | unusable_col | unknown_col | error_math | error_vector_expr | error_not_nonboolean | error_between_expr | error_aggr

    // These are the raw columns in the selectable
    {make_columns(columns)}

    {gather_columns("unusable_col", columns, "unusable", [])}
    {gather_columns("date.1", columns, "date", ["date_conv", "day_conv", "week_conv", "month_conv", "quarter_conv", "year_conv", "datetime_to_date_conv", "date_aggr"])}
    {gather_columns("datetime.2", columns, "datetime", ["datetime_conv"])}
    // Datetimes that are converted to the end of day
    {gather_columns("datetime_end.1", columns, "datetime", ["datetime_end_conv", "datetime_aggr"])}
    {gather_columns("boolean.1", columns, "bool", ["TRUE", "FALSE", "bool_expr", "vector_expr", "between_expr", "not_boolean", "or_boolean", "and_boolean", "paren_boolean", "intelligent_date_expr", "intelligent_datetime_expr"])}
    {gather_columns("string.1", columns, "str", ["ESCAPED_STRING", "string_add", "string_cast"])}
    {gather_columns("num.1", columns, "num", ["NUMBER", "num_add", "num_sub", "num_mul", "num_div", "int_cast", "aggr", "error_aggr"])}
    string_add: string "+" string                
    num_add.1: num "+" num | "(" num "+" num ")"                      
    num_sub.1: num "-" num | "(" num "-" num ")"
    num_mul.2: num "*" num | "(" num "*" num ")"
    num_div.2: num "/" num | "(" num "/" num ")"
    add: col "+" col

    // Various error conditions (fields we don't recognize, bad math)
    // Low priority matching of any [columnname] values
    unknown_col.0: "[" + NAME + "]"
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
    bool_expr: date comparator date | datetime comparator datetime | col comparator col | col null_comparator NULL
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
    vector_expr.1: string vector_comparator stringarray | num vector_comparator numarray
    vector_comparator.1: NOT? IN    
    stringarray.1: "(" [ESCAPED_STRING ("," ESCAPED_STRING)*] ","? ")"  -> consistent_array
    numarray.1: "(" [NUMBER ("," NUMBER)*] ","?  ")"                    -> consistent_array
    
    // Date
    date_conv.3: /date/i "(" ESCAPED_STRING ")"
    datetime_to_date_conv.3: /date/i "(" datetime ")"  -> day_conv
    datetime_conv.2: /date/i "(" ESCAPED_STRING ")"
    datetime_end_conv.1: /date/i "(" ESCAPED_STRING ")"

    // Conversions
    // date->date
    day_conv: /day/i "(" (date | datetime) ")"
    week_conv: /week/i "(" (date | datetime) ")"
    month_conv: /month/i "(" (date | datetime) ")"
    quarter_conv: /quarter/i "(" (date | datetime) ")"
    year_conv: /year/i "(" (date | datetime) ")"
    // col->string
    string_cast: /string/i "(" col ")"
    // col->int
    int_cast: /int/i "(" col ")"
    // date->int
    // TODO: age_conv: /age/i "(" (date | datetime) ")"    
    // TODO: date - date => int

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
    // Aggregations that return dates
    date_aggr.1: min_date_aggr | max_date_aggr
    min_date_aggr.1: /min/i "(" date ")"            -> min_aggr
    max_date_aggr.1: /max/i "(" date ")"            -> max_aggr
    // Aggregations that return datetimes
    datetime_aggr.1: min_datetime_aggr | max_datetime_aggr
    min_datetime_aggr.1: /min/i "(" datetime ")"    -> min_aggr
    max_datetime_aggr.1: /max/i "(" datetime ")"    -> max_aggr

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

    print(grammar)
    return columns, grammar


class ErrorVisitor(Visitor):
    """Raise descriptive exceptions for any errors found """

    def __init__(self, text, forbid_aggregation, drivername):
        super().__init__()
        self.text = text
        self.forbid_aggregation = forbid_aggregation
        self.aggregation = False
        self.errors = []
        self.drivername = drivername

    def data_type(self, tree):
        # Find the data type for a tree
        if tree.data == "col":
            dt = self.data_type(tree.children[0])
        else:
            dt = tree.data
        if dt == "datetime_end":
            dt = "datetime"
        return dt

    def _add_error(self, message, tree):
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

    def error_add(self, tree):
        self._error_math(tree, "added together")

    def error_mul(self, tree):
        self._error_math(tree, "multiplied together")

    def error_sub(self, tree):
        self._error_math(tree, "subtracted")

    def error_div(self, tree):
        self._error_math(tree, "divided")

    def aggr(self, tree):
        self.aggregation = True
        if self.forbid_aggregation:
            self._add_error(f"Aggregations are not allowed in this ingredient.", tree)

    def unknown_col(self, tree):
        """Column name doesn't exist in the data """
        tok1 = tree.children[0]
        self._add_error(f"{tok1} is not a valid column name", tree)

    def unusable_col(self, tree):
        """Column name isn't a data type we can handle """
        tok1 = tree.children[0]
        self._add_error(
            f"{tok1} is a data type that can't be used. Usable data types are strings, numbers, boolean, dates, and datetimes",
            tree,
        )

    def error_not_nonboolean(self, tree):
        """NOT string or NOT num """
        self._add_error(f"NOT requires a boolean value", tree)

    def mixedarray(self, tree):
        """An array containing a mix of strings and numbers """
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
        """Aggregating a bad data type """
        fn = tree.children[0].children[0]
        dt = self.data_type(tree.children[0].children[1])
        self._add_error(
            f"A {dt} can not be aggregated using {fn}.",
            tree,
        )

    def error_between_expr(self, tree):
        col, BETWEEN, left, AND, right = tree.children
        col_type = self.data_type(col)
        left_type = self.data_type(left)
        right_type = self.data_type(right)
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
        """ a > b where the types of a and b don't match """
        left, _, right = tree.children
        if isinstance(left, Tree) and isinstance(right, Tree):
            tok1 = left.children[0]
            tok2 = right.children[0]
            if right.data == left.data == "date":
                return
            if right.data == left.data == "datetime":
                return
            if tok1.data != tok2.data:
                self._add_error(f"Can't compare {tok1.data} to {tok2.data}", tree)

    def percentile_aggr(self, tree):
        """Sum up the things """
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
    """Converts a field to a SQLAlchemy expression """

    def __init__(self, selectable, columns, forbid_aggregation=True):
        self.text = None
        self.selectable = selectable
        self.columns = columns
        self.forbid_aggregation = forbid_aggregation
        # Database driver
        try:
            self.drivername = selectable.metadata.bind.url.drivername
        except Exception:
            self.drivername = "unknown"

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
        """Cast a field to a string """
        return cast(fld, String())

    def num(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def int_cast(self, _, fld):
        """Cast a field to a string """
        return cast(fld, Integer())

    def boolean(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def num_add(self, a, b):
        """ Add numbers or strings """
        return a + b

    def string_add(self, a, b):
        """ Add numbers or strings """
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

    # Dates and datetimes

    def date(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def datetime(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

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
        """Truncate to mondays """
        if self.drivername == "bigquery":
            return func.date_trunc(fld, text("day"))
        else:
            # Postgres + redshift
            return func.date_trunc("day", fld)

    def week_conv(self, _, fld):
        """Truncate to mondays """
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
        """A comparator like =, !=, >, >= """
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
            is_comp = {
                "__eq__": "is_",
                "__ne__": "isnot",
            }
            comparator = is_comp.get(comparator, comparator)

        return getattr(left, comparator)(right)

    # Aggregations

    def aggr(self, v):
        return v

    def date_aggr(self, v):
        return v

    def datetime_aggr(self, v):
        return v

    def sum_aggr(self, _, fld):
        """Sum up the things """
        return func.sum(fld)

    def min_aggr(self, _, fld):
        """Sum up the things """
        return func.min(fld)

    def max_aggr(self, _, fld):
        """Sum up the things """
        return func.max(fld)

    def avg_aggr(self, _, fld):
        """Sum up the things """
        return func.avg(fld)

    def count_aggr(self, _, fld):
        """Sum up the things """
        if fld.data == "star":
            return func.count()
        else:
            return func.count(fld)

    def percentile_aggr(self, percentile, fld):
        """Sum up the things """
        percentile_val = int(percentile[len("percentile") :])
        if percentile_val not in (1, 5, 10, 25, 50, 75, 90, 95, 99):
            raise GrammarError(
                f"percentile values of {percentile_val} is not supported."
            )
        if self.drivername == "bigquery":
            # FIXME: This doesn't work
            return (func.percentile_cont(0.01).within_group(fld),)
            # return func.date_trunc(fld, text("day"))
        elif self.drivername == "sqlite":
            raise GrammarError("Percentile is not supported on sqlite")
        else:
            # Postgres + redshift
            return (func.percentile_cont(0.01).within_group(fld),)
            # return func.date_trunc("day", fld)

    def count_distinct_aggr(self, _, fld):
        """Sum up the things """
        return func.count(distinct(fld))

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


class Builder(object):
    def __init__(self, selectable, forbid_aggregation=False):
        self.selectable = selectable
        self.forbid_aggregation = forbid_aggregation
        self.columns, self.grammar = make_grammar_for_table(selectable)
        self.parser = Lark(
            self.grammar,
            parser="earley",
            ambiguity="resolve",
            start="col",
            propagate_positions=True,
        )
        self.transformer = TransformToSQLAlchemyExpression(
            self.selectable, self.columns
        )

        # Database driver
        try:
            self.drivername = selectable.metadata.bind.url.drivername
        except Exception:
            self.drivername = "unknown"

    def raw_sql(self, c):
        """Utility to print sql for a expression """
        return str(c.compile(compile_kwargs={"literal_binds": True}))

    def parse(self, text, debug=False):
        """Return a parse tree for text"""
        tree = self.parser.parse(text)
        error_visitor = ErrorVisitor(text, self.forbid_aggregation, self.drivername)
        error_visitor.visit(tree)
        if error_visitor.errors:
            if debug:
                print("".join(error_visitor.errors))
                print("Tree:\n" + tree.pretty())
            raise Exception("".join(error_visitor.errors))
        else:
            if debug:
                print("Tree:\n" + tree.pretty())
            self.transformer.text = text
            t = self.transformer.transform(tree)
            return t
