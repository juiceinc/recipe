import enum
from lark import Lark, Transformer, Visitor, v_args, UnexpectedInput
from sqlalchemy import String, Float, Date, DateTime, Boolean, Integer, case
from tests.test_base import Scores2
from collections import defaultdict
boolean_columns = "bool_a bool_b".split()
string_columns = "str_a str_b".split()
number_columns = "num_a num_b".split()
data_columns = "date_a"
columns = boolean_columns + string_columns + number_columns

def convert(lst):
    return ["\[" + v + "\]" for v in lst]

from tests.test_base import Scores2


# grammar = """
#     col: boolean | string | num | error_col | error_add

#     // These are my raw columns
#     error_col.99: "[" + NAME + "]"

#     boolean: TRUE | FALSE | bool_col_1 | bool_col_2
#     string_add: string "+" string
#     string: ESCAPED_STRING | str_col_1 | str_col_2 | string_add
#     num_add: num "+" num
#     num: NUMBER | num_col_1 | num_col_2 | num_add
#     error_add: string "+" num | num "+" string 

#     TRUE: /TRUE/i
#     FALSE: /FALSE/i
#     NOTIN: NOT IN
#     OR: /OR/i
#     AND: /AND/i
#     NOT: /NOT/i
#     EQ: "="
#     NE: "!="
#     LT: "<"
#     LTE: "<="
#     GT: ">"
#     GTE: ">="
#     IN: /IN/i
#     IS: /IS/i
#     BETWEEN: /BETWEEN/i
#     NULL: /NULL/i
#     COMMENT: /#.*/

#     %import common.CNAME                       -> NAME
#     %import common.SIGNED_NUMBER               -> NUMBER
#     %import common.ESCAPED_STRING
#     %import common.WS_INLINE
#     %ignore COMMENT
#     %ignore WS_INLINE
# """


def make_columns(columns):
    """Return a lark string that looks like 

    // These are my raw columns
    str_0: "[" + /username/i + "]"
    str_1: "[" + /department/i + "]"
    str_2: "[" + /testid/i + "]"    
    """
    items = []
    for k in sorted(columns.keys()):
        c = columns[k]
        items.append(f'    {k}: "[" + /{c.name}/i + "]"')
    return "\n".join(items).lstrip()


def type_defn(columns, prefix, additions=None):
    if additions is None:
        additions = []
    matching_keys = [k for k in sorted(columns.keys()) if k.startswith(prefix + "_")]
    return " | ".join(matching_keys + additions)


def make_grammar_for_table(tbl):
    columns = {}

    type_counter = defaultdict(int)
    sqlalchemy_type_lookup = {
        String: "str",
        Boolean: "bool",
        Date: "date",
        DateTime: "datetime",
        Integer: "num",
        Float: "num",
    }

    for c in tbl.columns:
        prefix = sqlalchemy_type_lookup.get(type(c.type), "unusable")
        cnt = type_counter[prefix]
        type_counter[prefix] += 1
        columns[f"{prefix}_{cnt}"] = c


    grammar = f"""
    col: boolean | string | num | error_col | error_add

    // These are my raw columns
    {make_columns(columns)}
    error_col.99: "[" + NAME + "]"

    boolean: {type_defn(columns, "bool", ["TRUE", "FALSE"])}
    string_add: string "+" string
    string: {type_defn(columns, "str", ["ESCAPED_STRING", "string_add"])}
    num_add: num "+" num
    num: {type_defn(columns, "num", ["NUMBER", "num_add"])}
    error_add: string "+" num | num "+" string 

    TRUE: /TRUE/i
    FALSE: /FALSE/i
    NOTIN: NOT IN
    OR: /OR/i
    AND: /AND/i
    NOT: /NOT/i
    EQ: "="
    NE: "!="
    LT: "<"
    LTE: "<="
    GT: ">"
    GTE: ">="
    IN: /IN/i
    IS: /IS/i
    BETWEEN: /BETWEEN/i
    NULL: /NULL/i
    COMMENT: /#.*/

    %import common.CNAME                       -> NAME
    %import common.SIGNED_NUMBER               -> NUMBER
    %import common.ESCAPED_STRING
    %import common.WS_INLINE
    %ignore COMMENT
    %ignore WS_INLINE
"""
    print(grammar)
    return grammar


grammar = make_grammar_for_table(Scores2)

good_examples = """
[score]
[ScORE]
[username] + [department]
"foo" + [department]
[score] + [score]
1.0 + [score]
[score] + -1.0
"""

bad_examples = """
[foo_b]
[username] + [score]
"""


class ErrorVisitor(Visitor):
    """Raise descriptive exceptions for any errors found """
    def __init__(self, text):
        super().__init__()
        self.text = text

    def error_add(self, tree):
        # children = [child.data for child in tree.children]
        tok1 = tree.children[0]
        tok2 = tree.children[1]
        print(f"{tok1.data} and {tok2.data} can not be added together")
        return
        raise Exception(f"{tok1.data} and {tok2.data} can not be added together")

    def error_col(self, tree):
        """Column name doesn't exist in the data """
        tok1 = tree.children[0]
        # print(f"{tok1.data} and {tok2.data} can not be added together")
        print(f"{tok1} is not a valid column name")
        return
        raise Exception(f"{tok1} is not a valid column name")
    

class Builder(object):
    def __init__(self, selectable, require_aggregation=False):
        self.selectable = selectable
        self.require_aggregation = require_aggregation
        self.grammar = make_grammar_for_table(selectable)
        self.parser = Lark(self.grammar, parser="earley", ambiguity="resolve", start="col")

        # Database driver
        try:
            self.drivername = selectable.metadata.bind.url.drivername
        except Exception:
            self.drivername = "unknown"

    def parse(self, text):
        """Return a parse tree for text"""
        tree = self.parser.parse(text)
        error_visitor = ErrorVisitor(text)
        error_visitor.visit(tree)
        return tree


b = Builder(Scores2)


# field_parser = Lark(grammar, parser="earley", ambiguity="resolve", start="col")
for row in good_examples.split("\n"):
    if row:
        print(row)
        tree = b.parse(row)
        print(tree.pretty())

for row in bad_examples.split("\n"):
    if row:
        print(row)
        tree = b.parse(row)
        print(tree.pretty())





@v_args(inline=True)  # Affects the signatures of the methods
class TransformToSQLAlchemyExpression(Transformer):
    """Converts a field to a SQLAlchemy expression """

    # We have rules named "add", "sub", "mul", and "neg" in our grammar; Transformer
    # dispatches to these.
    from operator import add, sub, mul, neg

    def __init__(self, selectable, require_aggregation=False):
        self.selectable = selectable
        self.require_aggregation = require_aggregation
        # Database driver
        try:
            self.drivername = selectable.metadata.bind.url.drivername
        except Exception:
            self.drivername = "unknown"

    def number(self, value):
        try:
            return int(value)
        except ValueError:
            return float(value)

    def true(self, value):
        return True

    def false(self, value):
        return False

    def IN(self, value):
        return "IN"

    def NOTIN(self, value):
        return "NOTIN"

    def IS(self, value):
        return "IS"

    def NULL(self, value):
        return None

    def BETWEEN(self, value):
        return "BETWEEN"

    def aggregate(self, name):
        """Return a callable that generates SQLAlchemy to aggregate a field.

        Aggregations may be database specific
        """
        ag = self.aggregations.get(name.lower())
        if ag is None:
            raise ValueError(
                "Aggregation {} is not supported on {} engine".format(
                    name, self.drivername
                )
            )
        return ag

    def div(self, num, denom):
        """SQL safe division"""
        if isinstance(denom, (int, float)):
            if denom == 0:
                raise ValueError("Denominator can not be zero")
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

    def column(self, name):
        return find_column(self.selectable, name)

    def agex(self, aggr, val):
        if val == "*":
            return func.count()
        else:
            return aggr(val)

    def conversion(self, name):
        conv_fn = self.conversions.get(name.lower())
        if conv_fn is None:
            raise ValueError(
                "Conversion {} is not supported on {} engine".format(
                    conv_fn, self.drivername
                )
            )
        return conv_fn

    def convertedcol(self, conversion, col):
        conv_fn = self.conversions.get(conversion.lower())
        if conv_fn is None:
            raise ValueError(
                "Conversion {} is not supported on {} engine".format(
                    conv_fn, self.drivername
                )
            )
        return conv_fn(col)

    def expr(self, expr):
        return expr

    def case(self, *args):
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

    def is_comparison(self, *args):
        """Things that can be compared with IS

        will be either IS NULL or an intelligent date
        like "IS prior year" or "IS next week".

        If intelligent dates are used, determine the relevant dates and
        return a tuple of start_date, end_date
        """
        if len(args) == 1 and args[0] is None:
            return None
        else:
            # INTELLIGENT_DATE_OFFSET: /prior/i | /current/i | /next/i
            # INTELLIGENT_DATE_UNITS: /ytd/i | /year/i | /qtr/i | /month/i | /week/i | /day/i
            offset, units = str(args[0]).lower(), str(args[1]).lower()
            return calc_date_range(offset, units, date.today())

    def relation_expr(self, left, rel, right):
        rel = rel.lower()
        comparators = {
            "=": "__eq__",
            ">": "__gt__",
            ">=": "__ge__",
            "!=": "__ne__",
            "<": "__lt__",
            "<=": "__le__",
        }
        if right is None:
            return
        # Convert the right into a type compatible with the left
        right = convert_value(left, right)
        return getattr(left, comparators[rel])(right)

    def relation_expr_using_is(self, left, rel, right):
        """A relation expression like age is null or
        birth_date is last month"""
        # TODO: Why is this not handled by the tokenization
        if str(right).upper() == "NULL" or right is None:
            return left.is_(None)
        else:
            return left.between(*right)

    def array(self, *args):
        # TODO  check these are all the same type
        # And match the type of the column!
        return args

    def vector_relation_expr(self, left, rel, right):
        comparators = {"IN": "in_", "NOTIN": "notin_"}
        return getattr(left, comparators[rel.upper()])(right)

    def between_relation_expr(self, col, between, low, _, high):
        # TODO: check data types and convert dates.
        return col.between(convert_value(col, low), convert_value(col, high))

    def bool_expr(self, *exprs):
        if len(exprs) > 1:
            left, _, right = exprs
            return or_(left, right)
        else:
            return exprs[0]

    def string_literal(self, value):
        # Strip the quotes off of this string
        return value.value[1:-1]

    def bool_term(self, *exprs):
        if len(exprs) > 1:
            left, _, right = exprs
            return and_(left, right)
        else:
            return exprs[0]

    def not_bool_factor(self, notval, expr):
        return not_(expr)
