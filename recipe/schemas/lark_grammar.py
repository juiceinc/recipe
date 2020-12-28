import enum
from lark import Lark, Transformer, Visitor, v_args, UnexpectedInput, Tree
from lark.lexer import Token
from lark.visitors import inline_args
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


def make_grammar_for_table(selectable):
    """Build a dict of usable columns and a grammar for this selectable """

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

    for c in selectable.columns:
        prefix = sqlalchemy_type_lookup.get(type(c.type), "unusable")
        cnt = type_counter[prefix]
        type_counter[prefix] += 1
        columns[f"{prefix}_{cnt}"] = c

    grammar = f"""
    col: boolean | string | num | error_col | error_add

    // These are the raw columns in the selectable
    {make_columns(columns)}

    boolean.1: {type_defn(columns, "bool", ["TRUE", "FALSE"])}
    string_add: string "+" string
    string.1: {type_defn(columns, "str", ["ESCAPED_STRING", "string_add"])}
    num_add: num "+" num
    num_sub: num "-" num
    num_mul: num "*" num
    num.1: {type_defn(columns, "num", ["NUMBER", "num_add", "num_sub", "num_mul"])}


    // Low priority matching of any [columnname] values
    error_col.0: "[" + NAME + "]"
    error_add: string "+" num | num "+" string  | error_col "+" num

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
    return columns, grammar


grammar = make_grammar_for_table(Scores2)


good_examples = """
[score]
[ScORE]
[ScORE] + [ScORE]
[score] + 2.0
[username] + [department]
"foo" + [department]
1.0 + [score]
1.0 + [score] + [score]
[scores] + -1.0
-0.1 * [score] + 600
"""

bad_examples = """
[foo_b]
[username] + [score]
[score]   + [department]
"""


class ErrorVisitor(Visitor):
    """Raise descriptive exceptions for any errors found """

    def __init__(self, text):
        super().__init__()
        self.text = text
        self.errors = []

    def error_add(self, tree):
        # children = [child.data for child in tree.children]
        tok1 = tree.children[0]
        tok2 = tree.children[1]
        self.errors.append(f"{tok1.data} and {tok2.data} can not be added together")

    def error_col(self, tree):
        """Column name doesn't exist in the data """
        tok1 = tree.children[0]
        # print(f"{tok1.data} and {tok2.data} can not be added together")
        self.errors.append(f"{tok1} is not a valid column name")



@v_args(inline=True)  # Affects the signatures of the methods
class TransformToSQLAlchemyExpression(Transformer):
    """Converts a field to a SQLAlchemy expression """

    # We have rules named "add", "sub", "mul", and "neg" in our grammar; Transformer
    # dispatches to these.
    from operator import add, sub, mul, neg

    def __init__(self, selectable, columns, require_aggregation=False):
        self.selectable = selectable
        self.columns = columns
        self.require_aggregation = require_aggregation
        # Database driver
        try:
            self.drivername = selectable.metadata.bind.url.drivername
        except Exception:
            self.drivername = "unknown"

    def string(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        elif isinstance(v, Token):
            v = str(v)
            if v.startswith('"') and v.endswith('"'):
                return v[1:-1]
        else:
            return v

    def num(self, v):
        if isinstance(v, Tree):
            return self.columns[v.data]
        elif isinstance(v, Token):
            return float(v)
        else:
            return v

    def num_add(self, a, b):
        return a + b

    def num_sub(self, a, b):
        return a - b

    def num_mul(self, a, b):
        return a * b

    def string_add(self, a, b):
        return a + b

    def col(self, v):
        return v

    def TRUE(self, v):
        return True




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
        self.transformer = TransformToSQLAlchemyExpression(self.selectable, self.columns)

        # Database driver
        try:
            self.drivername = selectable.metadata.bind.url.drivername
        except Exception:
            self.drivername = "unknown"

    def raw_sql(self, c):
        """Utility to print sql for a expression """
        return str(c.compile(compile_kwargs={"literal_binds": True}))

    def parse(self, text):
        """Return a parse tree for text"""
        tree = self.parser.parse(text)
        error_visitor = ErrorVisitor(text)
        error_visitor.visit(tree)
        if error_visitor.errors:
            print("THERE WERE ERRORS\n" + "\n".join(error_visitor.errors))
            print(tree.pretty())
        else:
            print("Tree:\n" + tree.pretty())
            t = self.transformer.transform(tree)
            print("Raw sql: " + self.raw_sql(t))


b = Builder(Scores2)


# field_parser = Lark(grammar, parser="earley", ambiguity="resolve", start="col")
for row in good_examples.split("\n"):
    if row:
        print(f"\nInput: {row}")
        tree = b.parse(row)

print("\n\n\n" + "THESE ARE BAD\n" * 5)

for row in bad_examples.split("\n"):
    if row:
        print(row)
        tree = b.parse(row)