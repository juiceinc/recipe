from lark import Lark, Transformer, v_args

field_grammar = """
    ?start:  expr | bool_expr | partial_relation_expr
    ?expr: agex | sum                          -> expr
    ?agex: aggr "(" sum ")"
        | /count/i "(" STAR ")"                -> agex
    ?aggr:  /(sum|min|max|avg|count)/i         -> aggregate
    ?sum: product
        | sum "+" product                      -> add
        | sum "-" product                      -> sub
    ?product: atom
           | product "*" atom                  -> mul
           | product "/" atom                  -> div
    ?atom: agex
           | const
           | column
           | case
           | "(" sum ")"
    ?column:  NAME                             -> column
    ?const: NUMBER                             -> number
            | ESCAPED_STRING                   -> literal
            | /true/i                          -> true
            | /false/i                         -> false
            | /null/i                          -> null

    // Pairs of boolean expressions and expressions
    // forming case when {BOOL_EXPR} then {EXPR}
    // an optional final expression is the else.T
    ?case: "if" "(" (bool_expr "," expr)+ ("," expr)? ")"

    // boolean expressions
    ?bool_expr: bool_term ["OR" bool_term]
    ?bool_term: bool_factor ["AND" bool_factor]
    ?bool_factor: column
                  | "NOT" bool_factor          -> not_bool_factor
                  | "(" bool_expr ")"
                  | relation_expr
                  | vector_relation_expr
    ?partial_relation_expr: comparator atom
                | vector_comparator array
                | BETWEEN pair_array
    ?relation_expr:        atom comparator atom
    ?vector_relation_expr: atom vector_comparator array
                         | atom BETWEEN pair_array
    ?pair_array:           "(" const "," const ")"      -> array
    ?array:                "(" [const ("," const)*] ")"
    ?comparator: EQ | NE | LT | LTE | GT | GTE
    ?vector_comparator: IN | NOTIN
    EQ: "="
    NE: "!="
    LT: "<"
    LTE: "<="
    GT: ">"
    GTE: ">="
    IN: /IN/i
    NOTIN: /NOT/i /IN/i
    BETWEEN: /BETWEEN/i
    STAR: "*"
    COMMENT: /#.*/

    %import common.CNAME                       -> NAME
    %import common.SIGNED_NUMBER               -> NUMBER
    %import common.ESCAPED_STRING
    %import common.WS_INLINE
    %ignore COMMENT
    %ignore WS_INLINE
"""


field_parser = Lark(field_grammar, parser="earley")
