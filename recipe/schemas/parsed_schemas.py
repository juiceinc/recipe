"""Shelf config _version="2" supports parsed fields using a lark parser."""
import attr
from lark.exceptions import LarkError
from six import string_types
import logging
from sureberus import schema as S

from .utils import coerce_format, coerce_pop_version, _chain, SCALAR_TYPES
from .field_grammar import (
    field_parser,
    full_condition_parser,
    noag_field_parser,
    noag_full_condition_parser,
    noag_any_condition_parser,
)

logging.captureWarnings(True)


@attr.s
class ParseValidator(object):
    """A sureberus validator that checks that a field parses and matches
    certain tokens"""

    #: Message to display on failure
    parser = attr.ib(default=field_parser)

    def __call__(self, f, v, e):
        """Check parsing"""
        try:
            tree = self.parser.parse(v)
        except LarkError as exc:
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
            for k in sorted(keys_to_move):
                value["extra_fields"].append(
                    {"name": k[:-6] + "_expression", "field": value.pop(k)}
                )

    return value


def coerce_replace_refs(shelf):
    """Replace a reference in a field like @foo with the contents of foo's field"""
    replacements = []
    for k, v in shelf.items():
        if "field" in v:
            replacements.append(("@" + k, v["field"]))

    # Sort in descending order of length then by k
    replacements.sort(key=lambda k: (-1 * len(k[0]), k[0]))

    # Search for fields and replace with their replacements
    for ingr in shelf.values():
        for k in ingr.keys():
            if k == "field" or k.endswith("_field"):
                v = ingr[k]
                if isinstance(v, string_types) and "@" in v:
                    for search_for, replace_with in replacements:
                        v = v.replace(search_for, replace_with)
                    ingr[k] = v

    return shelf


def _stringify(value):
    """Convert a value into a string that will parse"""
    if value is None:
        return "NULL"
    elif value is True:
        return "TRUE"
    elif value is False:
        return "FALSE"
    elif isinstance(value, string_types):
        return '"' + value.replace('"', '\\"') + '"'
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
        parts.append(str(9999))
    else:
        parts.append(_stringify(bucket_default_label))

    return "if(" + ",".join(parts) + ")"


def _convert_partial_conditions(value):
    """Convert all partial conditions to full conditions in buckets and quickselects."""
    field = value.get("field")
    # Convert all bucket conditions to full conditions
    for itm in value.get("buckets", []):
        tree = noag_any_condition_parser.parse(itm["condition"])
        if tree.data == "partial_relation_expr":
            itm["condition"] = field + itm["condition"]
    # Convert all quickselects conditions to full conditions
    for itm in value.get("quickselects", []):
        tree = noag_any_condition_parser.parse(itm["condition"])
        if tree.data == "partial_relation_expr":
            itm["condition"] = "(" + field + ")" + itm["condition"]
    return value


def create_buckets(value):
    """If a validated bucket exists, convert it into a field and extra order by field."""
    buckets = value.pop("buckets", None)
    buckets_default_label = value.pop("buckets_default_label", None)
    if buckets:
        # Create a bucket
        if "extra_fields" not in value:
            value["extra_fields"] = []
        bucket_field = _convert_bucket_to_field(buckets, buckets_default_label)
        bucket_order_by_field = _convert_bucket_to_field(
            buckets, buckets_default_label, use_indices=True
        )
        value["field"] = bucket_field
        value["extra_fields"].append(
            {"name": "order_by_expression", "field": bucket_order_by_field}
        )
        pass
    return value


def ensure_aggregation(fld):
    """ Ensure that a field has an aggregation by wrapping the entire field
    in a sum if no aggregation is supplied. """
    try:
        tree = field_parser.parse(fld)
        has_agex = list(tree.find_data("agex"))
        if has_agex:
            return fld
        else:
            return "sum(" + fld + ")"
    except LarkError:
        # If we can't parse we will handle this in the validator
        return fld


def add_version(v):
    # Add version to a parsed ingredient
    v["_version"] = "2"
    return v


# Sureberus validators that check how a field parses
validate_parses_with_agex = ParseValidator(parser=field_parser)
validate_parses_without_agex = ParseValidator(parser=noag_field_parser)
validate_any_condition = ParseValidator(parser=noag_any_condition_parser)
validate_condition = ParseValidator(parser=noag_full_condition_parser)
validate_agex_condition = ParseValidator(parser=full_condition_parser)


# A field that may OR MAY NOT contain an aggregation.
# It will be the transformers responsibility to add an aggregation if one is missing
agex_field_schema = S.String(
    required=True, validator=validate_parses_with_agex, coerce=ensure_aggregation
)

# A field that is guaranteed to not contain an aggregation
noag_field_schema = S.String(required=True, validator=validate_parses_without_agex)

# A full condition guaranteed to not contain an aggregation
condition_schema = S.String(required=True, validator=validate_condition)

# A full or partial contain guaranteed to not contain an aggregation
any_condition_schema = S.String(required=True, validator=validate_any_condition)

# A full condition guaranteed to not contain an aggregation
labeled_condition_schema = S.Dict(
    schema={"condition": condition_schema, "label": S.String(required=True)}
)

# A full condition guaranteed to not contain an aggregation
named_condition_schema = S.Dict(
    schema={"condition": condition_schema, "name": S.String(required=True)}
)

format_schema = S.String(coerce=coerce_format, required=False)

metric_schema = S.Dict(
    schema={
        "field": agex_field_schema,
        "format": format_schema,
        "quickselects": S.List(required=False, schema=labeled_condition_schema),
    },
    coerce=_convert_partial_conditions,
    coerce_post=add_version,
    allow_unknown=True,
)

dimension_schema = S.Dict(
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
        "quickselects": S.List(required=False, schema=named_condition_schema),
    },
    coerce=_chain(move_extra_fields, _convert_partial_conditions),
    coerce_post=_chain(create_buckets, add_version),
    allow_unknown=True,
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
    coerce=_chain(coerce_pop_version, coerce_replace_refs),
    allow_unknown=True,
)
