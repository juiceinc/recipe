import attr
import logging
from sureberus import schema as S
from sureberus import errors as E

from .utils import coerce_format, pop_version

from lark import Lark, Transformer, v_args

logging.captureWarnings(True)

SCALAR_TYPES = [S.Integer(), S.String(), S.Float(), S.Boolean()]


############################
# PARSED
# Shelf config _version="2" supports parsed fields.
############################

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


def parsed_move_fields(value):
    """ Move any fields that look like "{role}_field" into the extra_fields
    list. These will be processed as fields. Rename them as {role}_expression.
    """
    if isinstance(value, dict):
        keys_to_move = [k for k in value.keys() if k.endswith("_field")]
        if keys_to_move:
            value["extra_fields"] = []
            for k in keys_to_move:
                value["extra_fields"].append(
                    {"name": k[:-6] + "_expression", "field": value.pop(k)}
                )

    return value


def add_version(v):
    # Add version to a parsed ingredient
    v["_version"] = "2"
    return v


@attr.s
class TreeTester:
    """Test that a parse tree contains certain tokens returning boolean
    """

    #: Must begin with one of these tokens
    required_head_tokens = attr.ib(default=[])
    #: Must not contain any of these tokens anywhere
    forbidden_tokens = attr.ib(default=[])
    #: Must contain at least one of these tokens
    required_tokens = attr.ib(default=[])
    and_other = attr.ib(default=None)
    or_other = attr.ib(default=None)

    def __and__(self, other):
        self.and_other = other
        return self

    def __or__(self, other):
        self.or_other = other
        return self

    def __call__(self, tree):
        result = True
        if self.required_head_tokens:
            if tree.data not in self.required_head_tokens:
                result = False
        if result and self.forbidden_tokens:
            for tok in self.forbidden_tokens:
                if list(tree.find_data(tok)):
                    result = False
                    break
        if result and self.required_tokens:
            for tok in self.required_tokens:
                if not list(tree.find_data(tok)):
                    result = False
                    break

        if result and self.and_other:
            result = result and self.and_other(tree)

        if not result and self.or_other:
            result = result or self.or_other(tree)

        return result


@attr.s
class ParseValidator:
    """A sureberus validator that checks that a field parses and matches
    certain tokens"""

    #: Message to display on failure
    msg = attr.ib()
    tester = attr.ib()

    def __call__(self, f, v, e):
        """Check parsing"""
        try:
            tree = field_parser.parse(v)
            if not self.tester(tree):
                failure_message = str(v) + " " + self.msg
                raise e(f, failure_message)
        except Exception as exc:
            # A Lark error message raised when the value doesn't parse
            raise exc


# Testers for parsed fields and conditions
# These test the fields parse and match certain conditions
test_agex = TreeTester(required_tokens=["agex"])
test_no_agex = TreeTester(forbidden_tokens=["agex"])
test_any_condition = TreeTester(
    required_head_tokens=[
        "partial_relation_expr",
        "bool_expr",
        "bool_term",
        "bool_factor",
        "relation_expr",
    ]
)
test_full_condition = TreeTester(
    required_head_tokens=["bool_expr", "bool_term", "bool_factor", "relation_expr"]
)
test_any_condition_no_agex = test_any_condition & test_no_agex
test_full_condition_no_agex = test_full_condition & test_no_agex
test_full_condition_agex = test_full_condition & test_agex


# Sureberus validators that use the testers
validate_parses_with_agex = ParseValidator(
    msg="must contain an aggregate expression", tester=test_agex
)
validate_parses_without_agex = ParseValidator(
    msg="must not contain an aggregate expression", tester=test_no_agex
)
validate_any_condition = ParseValidator(
    msg="must be a condition or partial condition and not include an aggregation",
    tester=test_any_condition_no_agex,
)
validate_condition = ParseValidator(
    msg="must be a condition and not include an aggregation",
    tester=test_full_condition_no_agex,
)
validate_agex_condition = ParseValidator(
    msg="must be a condition and include an aggregation",
    tester=test_full_condition_agex,
)


agex_field_schema = S.String(required=True, validator=validate_parses_with_agex)

noag_field_schema = S.String(required=True, validator=validate_parses_without_agex)

condition_schema = S.String(required=True, validator=validate_condition)

any_condition_schema = S.String(required=True, validator=validate_any_condition)

labeled_condition_schema = S.Dict(
    schema={"condition": condition_schema, "label": S.String(required=True)}
)

format_schema = S.String(coerce=coerce_format, required=False)

metric_schema = S.Dict(
    schema={
        "field": agex_field_schema,
        "format": format_schema,
        "quickselects": S.List(required=False, schema=labeled_condition_schema),
    },
    coerce_post=add_version,
    allow_unknown=True,
)

dimension_schema = S.Dict(
    allow_unknown=True,
    coerce=parsed_move_fields,
    coerce_post=add_version,
    schema={
        "field": noag_field_schema,
        "extra_fields": S.List(
            required=False,
            schema=S.Dict(
                schema={"field": noag_field_schema, "name": S.String(required=True)}
            ),
        ),
        "buckets": S.List(required=False, schema=labeled_condition_schema),
        "buckets_default_label": {"anyof": SCALAR_TYPES, "required": False},
        "format": format_schema,
        "quickselects": S.List(required=False, schema=labeled_condition_schema),
    },
)

filter_schema = S.Dict(
    allow_unknown=True, coerce_post=add_version, schema={"condition": condition_schema}
)

having_schema = S.Dict(
    allow_unknown=True, coerce_post=add_version, schema={"condition": condition_schema}
)

ingredient_schema = S.Dict(
    choose_schema=S.when_key_is(
        "kind",
        {
            "Metric": metric_schema,
            "Dimension": dimension_schema,
            "Filter": filter_schema,
            "Having": having_schema,
        },
        default_choice="Metric",
    ),
    registry={},
)

shelf_schema = S.Dict(
    valueschema=ingredient_schema,
    keyschema=S.String(),
    coerce=pop_version,
    allow_unknown=True,
)
