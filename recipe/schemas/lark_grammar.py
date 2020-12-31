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
    inspection,
    not_,
    and_,
    or_,
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
    col: boolean | string | num | date | datetime_end | datetime | unusable_col | unknown_col | error_math | error_vector_expr | error_not_nonboolean

    // These are the raw columns in the selectable
    {make_columns(columns)}

    {gather_columns("unusable_col", columns, "unusable", [])}
    {gather_columns("date.1", columns, "date", ["date_conv"])}
    {gather_columns("datetime.2", columns, "datetime", ["datetime_conv"])}
    // Datetimes that are converted to the end of day
    {gather_columns("datetime_end.1", columns, "datetime", ["datetime_end_conv"])}
    {gather_columns("boolean.1", columns, "bool", ["TRUE", "FALSE", "bool_expr", "vector_expr", "between_expr", "not_boolean", "or_boolean", "and_boolean", "paren_boolean", "intelligent_date_expr", "intelligent_datetime_expr"])}
    {gather_columns("string.1", columns, "str", ["ESCAPED_STRING", "string_add"])}
    {gather_columns("num.1", columns, "num", ["NUMBER", "num_add", "num_sub", "num_mul", "num_div"])}
    string_add: string "+" string                -> add
    num_add.1: num "+" num                       -> add
    num_sub.1: num "-" num
    num_mul.1: num "*" num
    num_div.1: num "/" num
    add: col "+" col

    // Various error conditions (fields we don't recognize, bad math)
    // Low priority matching of any [columnname] values
    unknown_col.0: "[" + NAME + "]"
    error_math.0: error_add | error_sub | error_mul | error_div
    error_add.0: col "+" col
    error_sub.0: col "-" col
    error_mul.0: col "*" col
    error_div.0: col "/" col
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
    intelligent_date_expr: date IS INTELLIGENT_DATE_OFFSET INTELLIGENT_DATE_UNITS
    intelligent_datetime_expr: datetime IS INTELLIGENT_DATE_OFFSET INTELLIGENT_DATE_UNITS
    between_expr: string BETWEEN string AND string | num BETWEEN num AND num | date BETWEEN date AND date | datetime BETWEEN datetime AND datetime_end
    vector_expr: string vector_comparator stringarray | num vector_comparator numarray
    vector_comparator.1: NOT? IN    
    stringarray.1: "(" [ESCAPED_STRING ("," ESCAPED_STRING)*] ","? ")"  -> consistent_array
    numarray.1: "(" [NUMBER ("," NUMBER)*] ","?  ")"                    -> consistent_array
    
    // Date
    date_conv.3: /date/i "(" ESCAPED_STRING ")"
    datetime_conv.2: /date/i "(" ESCAPED_STRING ")"
    datetime_end_conv.1: /date/i "(" ESCAPED_STRING ")"

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

    def __init__(self, text):
        super().__init__()
        self.text = text
        self.allow_aggregations = True
        self.errors = []

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


@v_args(inline=True)  # Affects the signatures of the methods
class TransformToSQLAlchemyExpression(Transformer):
    """Converts a field to a SQLAlchemy expression """

    def __init__(self, selectable, columns, require_aggregation=False):
        self.text = None
        self.selectable = selectable
        self.columns = columns
        self.require_aggregation = require_aggregation
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

    def num(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def boolean(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        else:
            return v

    def add(self, a, b):
        """ Add numbers or strings """
        return a + b

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

        # TODO: Convert the right into a type compatible with the left
        # right = convert_value(left, right)
        return getattr(left, comparator)(right)

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
    def __init__(self, selectable, require_aggregation=False):
        self.selectable = selectable
        self.require_aggregation = require_aggregation
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
        error_visitor = ErrorVisitor(text)
        error_visitor.visit(tree)
        if error_visitor.errors:
            print("".join(error_visitor.errors))
            if debug:
                print("Tree:\n" + tree.pretty())
            raise Exception("".join(error_visitor.errors))
        else:
            if debug:
                print("Tree:\n" + tree.pretty())
            self.transformer.text = text
            t = self.transformer.transform(tree)
            return t
