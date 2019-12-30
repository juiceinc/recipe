from lark import Lark, Transformer, v_args
from .utils import aggregations
from ..compat import basestring

allowed_aggr_keys = "|".join(
    k for k in aggregations.keys() if isinstance(k, basestring)
)

# Grammar for boolean expressions
boolean_expr_grammar = """
    ?start: bool_expr | partial_relation_expr

    // Pairs of boolean expressions and expressions
    // forming case when {BOOL_EXPR} then {EXPR}
    // an optional final expression is the else.T
    ?case: "if" "(" (bool_expr "," expr ","?)+ (expr)? ")"

    // boolean expressions
    ?bool_expr: bool_term [OR bool_term]
    ?bool_term: bool_factor [AND bool_factor]
    ?bool_factor: NOT bool_factor                           -> not_bool_factor
                  | "(" bool_expr ")"
                  | relation_expr
                  | vector_relation_expr
                  | between_relation_expr
    ?partial_relation_expr.0: comparator atom
                | vector_comparator array
                | BETWEEN atom AND atom
    ?relation_expr.1:        atom comparator atom
    ?vector_relation_expr.1: atom vector_comparator array
    ?between_relation_expr.1: atom BETWEEN atom AND atom
    ?pair_array:           "(" const "," const ")"            -> array
    ?array:                "(" [const ("," const)*] ")"
    ?comparator: EQ | NE | LT | LTE | GT | GTE
    ?vector_comparator.1: IN | NOTIN
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
    NOTIN: NOT IN
    BETWEEN: /BETWEEN/i
"""

noag_field_grammar = (
    """
    ?expr: sum                                 -> expr
    ?sum: product
        | sum "+" product                      -> add
        | sum "-" product                      -> sub
    ?product: atom
           | product "*" atom                  -> mul
           | product "/" atom                  -> div
    ?atom: const
           | column
           | case
           | "(" sum ")"
    ?column.0:  NAME                           -> column
    ?const.1: NUMBER                           -> number
            | ESCAPED_STRING                   -> string_literal
            | /true/i                          -> true
            | /false/i                         -> false
            | /null/i                          -> null
    STAR: "*"
    COMMENT: /#.*/

    %import common.CNAME                       -> NAME
    %import common.SIGNED_NUMBER               -> NUMBER
    %import common.ESCAPED_STRING
    %import common.WS_INLINE
    %ignore COMMENT
    %ignore WS_INLINE
"""
    + boolean_expr_grammar
)


# Grammar for expressions that allow aggregations
# for instance:
# "sum(sales)" or "max(yards) - min(yards)"
# Aggregations are keys defined in
agex_field_grammar = (
    """
    ?expr: agex | sum                          -> expr
    ?agex: aggr "(" sum ")"
        | /count/i "(" STAR ")"                -> agex
    ?sum: product
        | sum "+" product                      -> add
        | sum "-" product                      -> sub
    ?product: atom
           | product "*" atom                  -> mul
           | product "/" atom                  -> div

    ?aggr:  /({allowed_aggr_keys})/i           -> aggregate
    ?atom: agex
           | const
           | column
           | case
           | "(" sum ")"
    ?column.0:  NAME                           -> column
    ?const.1: NUMBER                           -> number
            | ESCAPED_STRING                   -> string_literal
            | /true/i                          -> true
            | /false/i                         -> false
            | /null/i                          -> null

    STAR: "*"
    COMMENT: /#.*/

    %import common.CNAME                       -> NAME
    %import common.SIGNED_NUMBER               -> NUMBER
    %import common.ESCAPED_STRING
    %import common.WS_INLINE
    %ignore COMMENT
    %ignore WS_INLINE
""".format(
        allowed_aggr_keys=allowed_aggr_keys
    )
    + boolean_expr_grammar
)


ambig = "resolve"

# An expression that may contain aggregations
field_parser = Lark(agex_field_grammar, parser="earley", ambiguity=ambig, start="expr")

# A boolean expression ("x>5") that may contain aggregations
full_condition_parser = Lark(
    agex_field_grammar, parser="earley", ambiguity=ambig, start="bool_expr"
)

# An exprssion that may not contain aggregations
noag_field_parser = Lark(
    noag_field_grammar, parser="earley", ambiguity=ambig, start="expr"
)

# A full condition ("x>5") or a partial condition (">5") that may not contain
# aggregations
noag_any_condition_parser = Lark(noag_field_grammar, parser="earley", ambiguity=ambig)

# A partial condition (">5", "in (1,2,3)") that may not contain aggregations
noag_partial_condition_parser = Lark(
    noag_field_grammar, parser="earley", ambiguity=ambig, start="partial_relation_expr"
)

# A full condition ("x>5") that may not contain aggregations
noag_full_condition_parser = Lark(
    noag_field_grammar, parser="earley", ambiguity=ambig, start="bool_expr"
)
