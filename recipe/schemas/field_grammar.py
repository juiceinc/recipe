from lark import Lark
from .engine_support import aggregations_by_engine, conversions_by_engine
from .utils import convert_by_engine_keys_to_regex

# Generate a regex expression containing all the by_engine keys
allowed_aggr_keys = convert_by_engine_keys_to_regex(aggregations_by_engine)
allowed_conv_keys = convert_by_engine_keys_to_regex(conversions_by_engine)


base_field_grammar_args = {
    "allowed_aggr_keys": allowed_aggr_keys,
    "allowed_conv_keys": allowed_conv_keys,
}


# Base grammar for field expressions
# This grammar depends on a definition of atom which will be
# added in the specific field grammars
base_field_grammar = """
    ?sum: product
        | sum "+" product                      -> add
        | sum "-" product                      -> sub
    ?product: atom
           | product "*" atom                  -> mul
           | product "/" atom                  -> div
    ?conversion:  /({allowed_conv_keys})/i
    ?convertedcol: conversion "(" column ")"
    ?column.0: "[" NAME "]"     -> column
             | NAME             -> column

    ?start: bool_expr | partial_relation_expr

    // Pairs of boolean expressions and expressions
    // forming case when BOOL_EXPR then EXPR
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
                | IS is_comparison
    ?relation_expr.1:        atom comparator atom
                           | atom IS is_comparison   -> relation_expr_using_is
    ?vector_relation_expr.1: atom vector_comparator array
    ?between_relation_expr.1: atom BETWEEN atom AND atom
    ?array:                "(" [const ("," const)*] ")"
    ?comparator: EQ | NE | LT | LTE | GT | GTE
    ?vector_comparator.1: IN | NOTIN
    ?is_comparison.1: NULL | INTELLIGENT_DATE_OFFSET INTELLIGENT_DATE_UNITS
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
    INTELLIGENT_DATE_OFFSET: /prior/i | /last/i | /previous/i | /current/i | /this/i | /next/i
    INTELLIGENT_DATE_UNITS: /ytd/i | /year/i | /qtr/i | /month/i | /mtd/i | /day/i

    ?const.1: NUMBER                           -> number
            | ESCAPED_STRING                   -> string_literal
            | /true/i                          -> true
            | /false/i                         -> false
    STAR: "*"
    COMMENT: /#.*/

    %import common.CNAME                       -> NAME
    %import common.SIGNED_NUMBER               -> NUMBER
    %import common.ESCAPED_STRING
    %import common.WS_INLINE
    %ignore COMMENT
    %ignore WS_INLINE
""".format(
    **base_field_grammar_args
)

# A grammar that does not include aggregate expressions
noag_field_grammar = (
    """
    ?expr: sum                                 -> expr
    ?aggr:  /({allowed_aggr_keys})/i           -> aggregate
    ?atom: const
           | column
           | convertedcol
           | case
           | "(" sum ")"
""".format(
        **base_field_grammar_args
    )
    + base_field_grammar
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
    ?aggr:  /({allowed_aggr_keys})/i           -> aggregate
    ?atom: agex
           | const
           | column
           | convertedcol
           | case
           | "(" sum ")"
""".format(
        **base_field_grammar_args
    )
    + base_field_grammar
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
