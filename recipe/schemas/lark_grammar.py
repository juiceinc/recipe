from lark import Lark

boolean_columns = "bool_a bool_b".split()
string_columns = "str_a str_b".split()
number_columns = "num_a num_b".split()
columns = boolean_columns + string_columns + number_columns

def convert(lst):
    return ["\[" + v + "\]" for v in lst]



grammar = """
    col: boolean | string | num

    // These are my raw columns
    bool_col_1: /\[bool_a\]/i
    bool_col_2: /\[bool_b\]/i
    str_col_1: /\[str_a\]/i
    str_col_2: /\[str_b\]/i
    num_col_1: /\[num_a\]/i
    num_col_2: /\[num_b\]/i
    // trap_col: "[ + NAME + "]"

    boolean: TRUE | FALSE | bool_col_1 | bool_col_2
    string_add: string "+" string
    string: ESCAPED_STRING | str_col_1 | str_col_2 | string_add
    num_add: num "+" num
    num: NUMBER | num_col_1 | num_col_2 | num_add

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


good_examples = """
[num_a]
[NUM_a]
[str_a] + [str_b]
"foo" + [str_b]
[num_a] + [num_b]
1.0 + [num_b]
[num_b] + 1.0
"""

bad_examples = """
[str_a] + [num_a]
1.0 + [bool_a]
"""

field_parser = Lark(grammar, parser="earley", ambiguity="resolve", start="col")
for row in good_examples.split("\n"):
    if row:
        print(row)
        tree = field_parser.parse(row)
        print(tree.pretty())

# for row in bad_examples.split("\n"):
#     if row:
#         print(row)
#         tree = field_parser.parse(row)
#         print(tree.pretty())
