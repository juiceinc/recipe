"""
Registers recipe schemas
"""

import logging
import re

from copy import copy
from sureberus import schema as S
from .utils import (
    sqlalchemy_datatypes,
    coerce_format,
    SCALAR_TYPES,
    coerce_pop_version,
)
from .engine_support import aggregations

logging.captureWarnings(True)

default_aggregation = "sum"
no_aggregation = "none"


aggr_keys = "|".join(k for k in aggregations.keys() if isinstance(k, str))
# Match patterns like sum(a)
field_pattern = re.compile(r"^({})\((.*)\)$".format(aggr_keys))


def find_operators(value):
    """ Find operators in a field that may look like "a+b-c" """
    parts = re.split("[+-/*]", value)
    field, operators = parts[0], []
    if len(parts) == 1:
        return field, operators

    remaining_value = value[len(field) :]
    if remaining_value:
        for part in re.findall(r"[+-/*][@\w\.]+", remaining_value):
            # TODO: Full validation on other fields
            other_field = _coerce_string_into_field(
                part[1:], search_for_operators=False
            )
            operators.append({"operator": part[0], "field": other_field})
    return field, operators


def _coerce_string_into_field(value, search_for_operators=True):
    """Convert a string into a field, potentially parsing a functional
    form into a value and aggregation"""
    if isinstance(value, str):
        if value.startswith("@"):
            result = _coerce_string_into_field(value[1:])
            result["ref"] = result["value"]
            return result

        # Remove all whitespace
        value = re.sub(r"\s+", "", value, flags=re.UNICODE)
        m = re.match(field_pattern, value)
        if m:
            aggr, value = m.groups()
            operators = []
            if search_for_operators:
                value, operators = find_operators(value)
            result = {"value": value, "aggregation": aggr}
            if operators:
                result["operators"] = operators
            return result

        else:
            operators = []
            if search_for_operators:
                value, operators = find_operators(value)

            # Check for a number
            try:
                float(value)
                result = {"value": value, "_use_raw_value": True}
            except ValueError:
                result = {"value": value}
            if operators:
                result["operators"] = operators
            return result
    elif isinstance(value, dict):
        # Removing these fields which are added in validation allows
        # a schema to be validated more than once without harm
        value.pop("_aggregation_fn", None)
        return value
    else:
        return value


def _field_post(field):
    """Add sqlalchemy conversion helper info

    Convert aggregation -> _aggregation_fn,
    as -> _cast_to_datatype and
    default -> _coalesce_to_value"""
    if "as" in field:
        field["_cast_to_datatype"] = field.pop("as")

    if "default" in field:
        field["_coalesce_to_value"] = field.pop("default")

    return field


def _to_lowercase(value):
    if isinstance(value, str):
        return value.lower()
    else:
        return value


def _field_schema(aggr=True, required=True):
    """Make a field schema that either aggregates or doesn't. """
    if aggr:
        ag = S.String(
            required=False,
            allowed=list(aggregations.keys()),
            default=default_aggregation,
            nullable=True,
        )
    else:
        ag = S.String(
            required=False,
            allowed=[no_aggregation, None],
            default=no_aggregation,
            nullable=True,
        )

    operator = S.Dict(
        {"operator": S.String(allowed=["+", "-", "/", "*"]), "field": S.String()}
    )

    return S.Dict(
        schema={
            "value": S.String(),
            "aggregation": ag,
            "ref": S.String(required=False),
            "condition": "condition",
            "operators": S.List(schema=operator, required=False),
            # Performs a dividebyzero safe sql division
            "divide_by": S.Dict(required=False, schema="aggregated_field"),
            # Performs casting
            "as": S.String(
                required=False,
                allowed=list(sqlalchemy_datatypes.keys()),
                coerce=_to_lowercase,
            ),
            # Performs coalescing
            "default": {"anyof": SCALAR_TYPES, "required": False},
            # Should the value be used directly in sql
            "_use_raw_value": S.Boolean(required=False),
        },
        coerce=_coerce_string_into_field,
        coerce_post=_field_post,
        allow_unknown=False,
        required=required,
    )


class ConditionPost(object):
    """Convert an operator like 'gt', 'lt' into '_op' and '_op_value'
    for easier parsing into SQLAlchemy"""

    def __init__(self, operator, _op, scalar):
        self.operator = operator
        self._op = _op
        self.scalar = scalar

    def __call__(self, value):
        value["_op"] = self._op
        _op_value = value.get(self.operator)
        # Wrap in a list
        if not self.scalar:
            if not isinstance(_op_value, list):
                _op_value = [_op_value]
        value["_op_value"] = _op_value
        return value


def _condition_schema(operator, _op, scalar=True, aggr=False, label_required=False):
    """Build a schema that expresses an (optionally labeled) boolean
    expression.

    For instance:

    condition:
      field: age
      label: 'over 80'
      gt: 80

    """

    allowed_values = SCALAR_TYPES if scalar else SCALAR_TYPES + [S.List()]
    field = "aggregated_field" if aggr else "non_aggregated_field"

    cond_schema = {
        "field": field,
        "label": S.String(required=label_required),
        operator: {"anyof": allowed_values},
    }

    _condition_schema = S.Dict(
        allow_unknown=False,
        schema=cond_schema,
        coerce_post=ConditionPost(operator, _op, scalar),
    )
    return _condition_schema


def _coerce_string_into_condition_ref(cond):
    if isinstance(cond, str) and cond.startswith("@"):
        return {"ref": cond[1:]}
    elif isinstance(cond, dict):
        # Removing these fields which are added in validation allows
        # a schema to be validated more than once without harm
        cond.pop("_op", None)
        cond.pop("_op_value", None)

    return cond


def _full_condition_schema(**kwargs):
    """Conditions can be a field with an operator, like this yaml example

    condition:
        field: foo
        gt: 22

    Or conditions can be a list of and-ed and or-ed conditions

    condition:
        or:
            - field: foo
              gt: 22
            - field: foo
              lt: 0

    :param aggr: Build the condition with aggregate fields (default is False)
    """

    label_required = kwargs.get("label_required", False)

    # Handle conditions where there's an
    operator_condition = S.Dict(
        choose_schema=S.when_key_exists(
            {
                "gt": _condition_schema("gt", "__gt__", **kwargs),
                "gte": _condition_schema("gte", "__ge__", **kwargs),
                "ge": _condition_schema("ge", "__ge__", **kwargs),
                "lt": _condition_schema("lt", "__lt__", **kwargs),
                "lte": _condition_schema("lte", "__le__", **kwargs),
                "le": _condition_schema("le", "__le__", **kwargs),
                "eq": _condition_schema("eq", "__eq__", **kwargs),
                "ne": _condition_schema("ne", "__ne__", **kwargs),
                "like": _condition_schema("like", "like", **kwargs),
                "ilike": _condition_schema("ilike", "ilike", **kwargs),
                "in": _condition_schema("in", "in_", scalar=False, **kwargs),
                "notin": _condition_schema("notin", "notin", scalar=False, **kwargs),
                "between": _condition_schema(
                    "between", "between", scalar=False, **kwargs
                ),
                "or": S.Dict(
                    schema={
                        "or": S.List(schema="condition"),
                        "label": S.String(required=label_required),
                    }
                ),
                "and": S.Dict(
                    schema={
                        "and": S.List(schema="condition"),
                        "label": S.String(required=label_required),
                    }
                ),
                # A reference to another condition
                "ref": S.Dict(schema={"ref": S.String()}),
            }
        ),
        required=False,
        coerce=_coerce_string_into_condition_ref,
    )

    return {
        "registry": {
            "condition": operator_condition,
            "aggregated_field": _field_schema(aggr=True),
            "non_aggregated_field": _field_schema(aggr=False),
        },
        "schema_ref": "condition",
    }


def _move_buckets_to_field(value):
    """ Move buckets from a dimension into the field """
    # return value
    buckets = value.pop("buckets", None)
    buckets_default_label = value.pop("buckets_default_label", "Not found")
    if buckets:
        if "field" in value:
            value["field"]["buckets"] = buckets
            if buckets_default_label is not None:
                value["field"]["buckets_default_label"] = buckets_default_label
    return value


def _move_extra_fields(value):
    """Move any fields that look like "{role}_field" into the extra_fields
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

        if "buckets" in value:
            for b in value["buckets"]:
                if "field" not in b:
                    b["field"] = copy(value.get("field"))

    return value


def _adjust_kinds(value):
    """Ensure kind is lowercase with a default of "metric".

    Rewrite deprecated field definitions for DivideMetirc, WtdAvgMetric,
    IdValueDimension, LookupDimension.
    """
    if isinstance(value, dict):
        kind = value.get("kind", "metric").lower()
        # measure is a synonym for metric
        if kind == "measure":
            kind = "metric"
        if kind in ("idvaluedimension", "lookupdimension"):
            kind = "dimension"
        elif kind == "dividemetric":
            kind = "metric"
            value["field"] = value.pop("numerator_field")
            value["divide_by"] = value.pop("denominator_field")
        elif kind == "wtdavgmetric":
            kind = "metric"
            fld = value.pop("field")
            wt = value.pop("weight")
            # assumes both field and weight are strings
            value["field"] = "{}*{}".format(fld, wt)
            value["divide_by"] = wt

        value["kind"] = kind
    return value


def _replace_refs_in_field(fld, shelf):
    """ Replace refs in fields"""
    if "ref" in fld:
        ref = fld["ref"]
        if ref in shelf:
            # FIXME: what to do if you can't find the ref
            fld = shelf[ref]["field"]
    else:
        # Replace conditions and operators within the field
        if "buckets" in fld:
            for cond in fld["buckets"]:
                if "ref" in cond:
                    # Update the condition in place
                    cond_ref = cond.pop("ref")
                    # FIXME: what to do if you can't find the ref
                    # What if the field doesn't have a condition
                    new_cond = shelf[cond_ref]["field"].get("condition")
                    if new_cond:
                        for k in list(cond.keys()):
                            cond.pop(k)
                        cond.update(new_cond)
                        if "label" not in cond:
                            cond["label"] = cond_ref

        if "condition" in fld and isinstance(fld["condition"], dict):
            cond = fld["condition"]
            if "ref" in cond:
                cond_ref = cond["ref"]
                # FIXME: what to do if you can't find the ref
                # What if the field doesn't have a condition
                new_cond = shelf[cond_ref]["field"].get("condition")
                if new_cond is None:
                    fld.pop("condition", None)
                else:
                    fld["condition"] = new_cond

        if "operators" in fld:
            # Walk the operators and replace field references
            new_operators = [
                {
                    "operator": op["operator"],
                    "field": _replace_refs_in_field(op["field"], shelf),
                }
                for op in fld["operators"]
            ]
            fld["operators"] = new_operators

    return fld


def _process_ingredient(ingr, shelf):
    # TODO: Support condition references (to filters, dimension/metric
    #  quickselects, and to field conditions)
    for k, fld in ingr.items():
        if (k.endswith("field") or k == "divide_by") and isinstance(fld, dict):
            ingr[k] = _replace_refs_in_field(fld, shelf)


def _replace_references(shelf):
    """Iterate over the shelf and replace and field.value: @ references
    with the field in another ingredient"""
    for ingr in shelf.values():
        _process_ingredient(ingr, shelf)
    return shelf


condition_schema = _full_condition_schema(aggr=False)

quickselect_schema = S.List(
    required=False,
    schema=S.Dict(schema={"condition": "condition", "name": S.String(required=True)}),
)

ingredient_schema_choices = {
    "metric": S.Dict(
        allow_unknown=True,
        schema={
            "field": "aggregated_field",
            "divide_by": "optional_aggregated_field",
            "format": S.String(coerce=coerce_format, required=False),
            "quickselects": quickselect_schema,
        },
    ),
    "dimension": S.Dict(
        allow_unknown=True,
        coerce=_move_extra_fields,
        coerce_post=_move_buckets_to_field,
        schema={
            "field": "non_aggregated_field",
            "extra_fields": S.List(
                required=False,
                schema=S.Dict(
                    schema={
                        "field": "non_aggregated_field",
                        "name": S.String(required=True),
                    }
                ),
            ),
            "buckets": S.List(required=False, schema="labeled_condition"),
            "buckets_default_label": {"anyof": SCALAR_TYPES, "required": False},
            "format": S.String(coerce=coerce_format, required=False),  # noqa: E123
            "lookup": S.Dict(required=False),
            "quickselects": quickselect_schema,
        },
    ),
    "filter": S.Dict(allow_unknown=True, schema={"condition": "condition"}),
    "having": S.Dict(allow_unknown=True, schema={"condition": "having_condition"}),
}

# Create a full schema that uses a registry
ingredient_schema = S.Dict(
    choose_schema=S.when_key_is(
        "kind", ingredient_schema_choices, default_choice="metric"
    ),
    coerce=_adjust_kinds,
    registry={
        "aggregated_field": _field_schema(aggr=True, required=True),
        "optional_aggregated_field": _field_schema(aggr=True, required=False),
        "non_aggregated_field": _field_schema(aggr=False, required=True),
        "condition": _full_condition_schema(aggr=False, label_required=False),
        "labeled_condition": _full_condition_schema(aggr=False, label_required=True),
        "having_condition": _full_condition_schema(aggr=True, label_required=False),
    },
)


shelf_schema = S.Dict(
    valueschema=ingredient_schema,
    keyschema=S.String(),
    allow_unknown=True,
    coerce=coerce_pop_version,
    coerce_post=_replace_references,
)
