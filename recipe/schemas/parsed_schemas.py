"""Shelf config _version="2" supports parsed fields using a lark parser."""
import attr
from six import string_types
import logging
from sureberus import schema as S

from .utils import coerce_format, pop_version, _chain, TreeTester, SCALAR_TYPES
from .field_grammar import field_parser

logging.captureWarnings(True)


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


def move_extra_fields(value):
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


def _stringify(value):
    """Convert a value into a string that will parse"""
    if value is None:
        return "NULL"
    elif value is True:
        return "TRUE"
    elif value is False:
        return "FALSE"
    elif isinstance(value, string_types):
        return "'" + value.replace("'", "'") + "'"
    else:
        return str(value)


def _convert_bucket_to_field(bucket, bucket_default_label, use_indices=False):
    """Convert a bucket structure to a field structure.

    This assumes that all conditions have been converted from
    partial conditions like '<5' to full conditions like 'age<5'
    with _convert_partial_conditions
    """

    parts, idx = [], 0
    for itm in bucket:
        cond = itm.get("condition")
        label = itm.get("label")

        parts.append(cond)
        if use_indices:
            parts.append(str(idx))
        else:
            # Stringify the label
            parts.append(_stringify(label))
        idx += 1

    # Add the default value
    if use_indices:
        parts.append(9999)
    else:
        parts.append(bucket_default_label)

    return "if(" + ",".join(parts) + ")"


def _convert_partial_conditions(value):
    """Convert all partial conditions to full conditions."""
    field = value.get("field")
    # Convert all bucket conditions to full conditions
    for itm in value.get("bucket", []):
        if is_partial_condition(itm["condition"]):
            itm["condition"] = field + itm["condition"]
    # Convert all quickselects conditions to full conditions
    for itm in value.get("quickselects", []):
        if is_partial_condition(itm["condition"]):
            itm["condition"] = field + itm["condition"]
    return value


def create_buckets(value):
    """If a validated bucket exists, convert it into a field and extra order by field."""
    bucket = value.pop("bucket", None)
    bucket_default_label = value.pop("buckets_default_label", None)
    if bucket:
        # Create a bucket
        if "extra_fields" not in value:
            value["extra_fields"] = []

        field = value.get("field")
        bucket_field = _convert_bucket_to_field(bucket, bucket_default_label)
        bucket_order_by_field = _convert_bucket_to_field(
            bucket, bucket_default_label, use_indices=True
        )
        value["field"] = bucket_field
        value["extra_fields"].append(
            {"name": "order_by_expression", "field": bucket_order_by_field}
        )
        pass
    return value


def add_version(v):
    # Add version to a parsed ingredient
    v["_version"] = "2"
    return v


# Test the fields parse and match certain conditions
is_agex = TreeTester(required_tokens=["agex"])
is_no_agex = TreeTester(forbidden_tokens=["agex"])
is_any_condition = TreeTester(
    required_head_token=[
        "partial_relation_expr",
        "bool_expr",
        "bool_term",
        "bool_factor",
        "relation_expr",
    ]
)
is_partial_condition = TreeTester(required_head_token=["partial_relation_expr"])
is_full_condition = TreeTester(
    required_head_token=["bool_expr", "bool_term", "bool_factor", "relation_expr"]
)
is_any_condition_no_agex = is_any_condition & is_no_agex
is_full_condition_no_agex = is_full_condition & is_no_agex
is_full_condition_agex = is_full_condition & is_agex


# Sureberus validators that use the testers
validate_parses_with_agex = ParseValidator(
    msg="must contain an aggregate expression", tester=is_agex
)
validate_parses_without_agex = ParseValidator(
    msg="must not contain an aggregate expression", tester=is_no_agex
)
validate_any_condition = ParseValidator(
    msg="must be a condition or partial condition and not include an aggregation",
    tester=is_any_condition_no_agex,
)
validate_condition = ParseValidator(
    msg="must be a condition and not include an aggregation",
    tester=is_full_condition_no_agex,
)
validate_agex_condition = ParseValidator(
    msg="must be a condition and include an aggregation", tester=is_full_condition_agex
)


# Schemas
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
    coerce_post=_chain(_convert_partial_conditions, add_version),
    allow_unknown=True,
)

dimension_schema = S.Dict(
    allow_unknown=True,
    coerce=move_extra_fields,
    coerce_post=_chain(_convert_partial_conditions, create_buckets, add_version),
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
