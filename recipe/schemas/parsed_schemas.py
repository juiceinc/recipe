"""Shelf config _version="2" supports parsed fields using a lark parser."""
from copy import deepcopy
from lark.exceptions import LarkError
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


def move_extra_fields(value):
    """Move any fields that look like "{role}_field" into the extra_fields
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
                if isinstance(v, str) and "@" in v:
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
    elif isinstance(value, str):
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
    if bucket_default_label is None:
        bucket_default_label = "Not found"
    if use_indices:
        parts.append(str(9999))
    else:
        parts.append(_stringify(bucket_default_label))

    return "if(" + ",".join(parts) + ")"


def _lowercase_kind(value):
    """Ensure kind is lowercase with a default of "metric" """
    if isinstance(value, dict):
        kind = value.get("kind", "metric").lower()
        # measure is a synonym for metric
        if kind == "measure":
            kind = "metric"
        value["kind"] = kind

    return value


def _save_raw_config(value):
    """Save the original config """
    value["_config"] = deepcopy(value)

    return value


def add_version(v):
    # Add version to a parsed ingredient
    v["_version"] = "2"
    return v



# A field that may OR MAY NOT contain an aggregation.
# It will be the transformers responsibility to add an aggregation if one is missing
field_schema = S.String(required=True)

labeled_condition_schema = S.Dict(
    schema={"condition": field_schema, "label": S.String(required=True)}
)

# A full condition guaranteed to not contain an aggregation
named_condition_schema = S.Dict(
    schema={"condition": field_schema, "name": S.String(required=True)}
)

format_schema = S.String(coerce=coerce_format, required=False)

metric_schema = S.Dict(
    schema={
        "field": field_schema,
        "format": format_schema,
        "quickselects": S.List(required=False, schema=labeled_condition_schema),
    },
    coerce_post=add_version,
    allow_unknown=True,
)

dimension_schema = S.Dict(
    schema={
        "field": field_schema,
        "extra_fields": S.List(
            required=False,
            schema=S.Dict(
                schema={"field": field_schema, "name": S.String(required=True)}
            ),
        ),
        "buckets": S.List(required=False, schema=labeled_condition_schema),
        "buckets_default_label": {"anyof": SCALAR_TYPES, "required": False},
        "format": format_schema,
        "lookup": S.Dict(required=False),
        "quickselects": S.List(required=False, schema=named_condition_schema),
    },
    coerce=move_extra_fields,
    coerce_post=add_version,
    allow_unknown=True,
)

filter_schema = S.Dict(
    allow_unknown=True, coerce_post=add_version, schema={"condition": field_schema}
)

having_schema = S.Dict(
    allow_unknown=True, coerce_post=add_version, schema={"condition": field_schema}
)

ingredient_schema = S.Dict(
    choose_schema=S.when_key_is(
        "kind",
        {
            "metric": metric_schema,
            "dimension": dimension_schema,
            "filter": filter_schema,
            "having": having_schema,
        },
        default_choice="metric",
    ),
    coerce=_chain(_lowercase_kind, _save_raw_config),
    registry={},
)

shelf_schema = S.Dict(
    valueschema=ingredient_schema,
    keyschema=S.String(),
    coerce=_chain(coerce_pop_version, coerce_replace_refs),
    allow_unknown=True,
)
