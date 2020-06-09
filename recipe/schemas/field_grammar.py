from lark import Lark, Transformer, v_args
from .utils import aggregations
from ..compat import basestring

aggr_keys = [k for k in aggregations.keys() if isinstance(k, basestring)]
# Sort the keys in descending order of length
aggr_keys.sort(key=lambda item: (len(item), item), reverse=True)
allowed_aggr_keys = "|".join(aggr_keys)


boolean_column_defn = "/NOMATCHBOOL/"
string_column_defn = "/NOMATCHSTRING/"
column_defn = "NAME"
base_field_grammar_args = {
    "allowed_aggr_keys": allowed_aggr_keys,
    "boolean_column_defn": boolean_column_defn,
    "string_column_defn": string_column_defn,
    "column_defn": column_defn,
}


# Base grammar for boolean expressions
# This grammar depends on a definition of atom which will be
# added in the field grammars
base_grammar = """
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
                | is_comparator NULL
    ?relation_expr.1:        atom comparator atom
                           | atom is_comparator NULL   -> relation_expr_using_is
    ?vector_relation_expr.1: atom vector_comparator array
    ?between_relation_expr.1: atom BETWEEN atom AND atom
    ?array:                "(" [const ("," const)*] ")"
    ?comparator: EQ | NE | LT | LTE | GT | GTE
    ?vector_comparator.1: IN | NOTIN
    ?is_comparator.1: IS | ISNOT
    ?is_comparison.1: NULL
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
    ISNOT: IS NOT
    NOTIN: NOT IN
    BETWEEN: /BETWEEN/i
    NULL: /NULL/i
    DYNAMIC_DATE_RELATIVE: /(prior|this|next)/i
    DYNAMIC_DATE_PERIOD: /(ytd|year|quarter|month|week|day)/i

    ?const.1: NUMBER                           -> number
            | ESCAPED_STRING                   -> string_literal
            | /true/i                          -> true
            | /false/i                         -> false
            | NULL
    STAR: "*"
    COMMENT: /#.*/

    %import common.CNAME                       -> NAME
    %import common.SIGNED_NUMBER               -> NUMBER
    %import common.ESCAPED_STRING
    %import common.WS_INLINE
    %ignore COMMENT
    %ignore WS_INLINE
"""
base_field_grammar = """
    ?sum: product
        | sum "+" product                      -> add
        | sum "-" product                      -> sub
    ?product: atom
           | product "*" atom                  -> mul
           | product "/" atom                  -> div
    ?column.0: {boolean_column_defn}           -> boolean_column
        | {string_column_defn}                 -> string_column
        | {column_defn}                        -> column
""".format(**base_field_grammar_args) + base_grammar


# A grammar that does not include aggregate expressions
noag_field_grammar = """
    ?expr: sum                                 -> expr
    ?atom: const
           | column
           | case
           | "(" sum ")"
""" + base_field_grammar


# Grammar for expressions that allow aggregations
# for instance:
# "sum(sales)" or "max(yards) - min(yards)"
# Aggregations are keys defined in
agex_field_grammar = """
    ?expr: agex | sum                          -> expr
    ?agex: aggr "(" sum ")"
        | /count/i "(" STAR ")"                -> agex
    ?aggr:  /({allowed_aggr_keys})/i           -> aggregate
    ?atom: agex
           | const
           | column
           | case
           | "(" sum ")"
""".format(**base_field_grammar_args) + base_field_grammar


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
