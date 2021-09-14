"""Constructors convert config schemas to sqlalchemy objects """
from copy import deepcopy
from sqlalchemy import and_, or_, func, cast, Float, case
from sureberus import normalize_schema
from sureberus import errors as E

from recipe.exceptions import BadIngredient
from .config_schemas import condition_schema, ingredient_schema, shelf_schema
from .utils import (
    sqlalchemy_datatypes,
    convert_value,
    find_column,
    ingredient_class_for_name,
)
from .engine_support import aggregations
from recipe.ingredients import InvalidIngredient


# constant used for ensuring safe division
SAFE_DIVISON_EPSILON = 0.000000001


def parse_validated_condition(cond, selectable):
    """Convert a validated condition into a SQLAlchemy boolean expression"""
    if cond is None:
        return

    if "and" in cond:
        conditions = []
        for c in cond.get("and", []):
            conditions.append(parse_validated_condition(c, selectable))
        return and_(*conditions)

    elif "or" in cond:
        conditions = []
        for c in cond.get("or", []):
            conditions.append(parse_validated_condition(c, selectable))
        return or_(*conditions)

    elif "field" in cond:
        field = parse_validated_field(cond.get("field"), selectable)
        _op = cond.get("_op")
        _op_value = convert_value(field, cond.get("_op_value"))

        if _op == "between":
            return getattr(field, _op)(*_op_value)
        else:
            return getattr(field, _op)(_op_value)


def parse_unvalidated_condition(cond, selectable):
    if cond is None:
        return
    try:
        cond = normalize_schema(condition_schema, cond, allow_unknown=False)
    except E.SureError as e:
        raise BadIngredient(str(e))
    return parse_validated_condition(cond, selectable)


def parse_unvalidated_field(unvalidated_fld, selectable, aggregated=True):
    kind = "Metric" if aggregated else "Dimension"
    ingr = {"field": unvalidated_fld, "kind": kind}
    try:
        ingr_dict = normalize_schema(ingredient_schema, ingr, allow_unknown=True)
    except E.SureError as e:
        raise BadIngredient(str(e))
    return parse_validated_field(ingr_dict["field"], selectable)


def ingredient_from_unvalidated_dict(unvalidated_ingr, selectable):
    try:
        ingr_dict = normalize_schema(
            ingredient_schema, unvalidated_ingr, allow_unknown=True
        )
    except E.SureError as e:
        raise BadIngredient(str(e))
    return create_ingredient_from_config(ingr_dict, selectable)


def parse_validated_field(fld, selectable, use_bucket_labels=True):
    """Converts a validated field to a sqlalchemy expression.
    Field references are looked up in selectable"""
    if fld is None:
        return

    fld = deepcopy(fld)

    if fld.pop("_use_raw_value", False):
        return float(fld["value"])

    if "buckets" in fld:
        # Buckets only appear in dimensions
        buckets_default_label = (
            fld.get("buckets_default_label") if use_bucket_labels else 9999
        )
        conditions = [
            (
                parse_validated_condition(cond, selectable),
                cond.get("label") if use_bucket_labels else idx,
            )
            for idx, cond in enumerate(fld.get("buckets", []))
        ]
        field = case(conditions, else_=buckets_default_label)
    else:
        field = find_column(selectable, fld["value"])

    operator_lookup = {
        "+": lambda fld: getattr(fld, "__add__"),
        "-": lambda fld: getattr(fld, "__sub__"),
        "/": lambda fld: getattr(fld, "__div__"),
        "*": lambda fld: getattr(fld, "__mul__"),
    }
    for operator in fld.get("operators", []):
        op = operator["operator"]
        other_field = parse_validated_field(operator["field"], selectable)
        if op == "/":
            other_field = (
                func.coalesce(cast(other_field, Float), 0.0) + SAFE_DIVISON_EPSILON
            )
        field = operator_lookup[op](field)(other_field)

    # Apply a condition if it exists
    cond = parse_validated_condition(fld.get("condition", None), selectable)
    if cond is not None:
        field = case([(cond, field)])

    # Lookup the aggregation function
    aggr_fn = aggregations.get(fld.get("aggregation"))
    field = aggr_fn(field)

    # lookup the sqlalchemy_datatypes
    cast_to_datatype = sqlalchemy_datatypes.get(fld.get("_cast_to_datatype"))
    if cast_to_datatype is not None:
        field = cast(field, cast_to_datatype)

    coalesce_to_value = fld.get("_coalesce_to_value")
    if coalesce_to_value is not None:
        field = func.coalesce(field, coalesce_to_value)

    return field


def create_ingredient_from_config(ingr_dict, selectable):
    """Create an ingredient from a validated config object."""
    kind = ingr_dict.pop("kind", "metric")
    IngredientClass = ingredient_class_for_name(kind.title())

    if IngredientClass is None:
        raise BadIngredient("Unknown ingredient kind")

    field_defn = ingr_dict.pop("field", None)
    divide_by_defn = ingr_dict.pop("divide_by", None)

    field = parse_validated_field(field_defn, selectable, use_bucket_labels=True)
    if isinstance(field_defn, dict) and "buckets" in field_defn:
        ingr_dict["order_by_expression"] = parse_validated_field(
            field_defn, selectable, use_bucket_labels=False
        )

    if divide_by_defn is not None:
        # Perform a divide by zero safe division
        divide_by = parse_validated_field(divide_by_defn, selectable)
        field = cast(field, Float) / (
            func.coalesce(cast(divide_by, Float), 0.0) + SAFE_DIVISON_EPSILON
        )

    quickselects = ingr_dict.pop("quickselects", None)
    parsed_quickselects = []
    if quickselects:
        for qf in quickselects:
            parsed_quickselects.append(
                {
                    "name": qf["name"],
                    "condition": parse_validated_condition(
                        qf.get("condition", None), selectable
                    ),
                }
            )
    ingr_dict["quickselects"] = parsed_quickselects

    args = [field]
    # Each extra field contains a name and a field
    for extra in ingr_dict.pop("extra_fields", []):
        ingr_dict[extra.get("name")] = parse_validated_field(
            extra.get("field"), selectable
        )

    try:
        return IngredientClass(*args, **ingr_dict)
    except BadIngredient as e:
        error = {
            "type": "bad_ingredient",
            "extra": {
                "details": str(e),
            },
        }
        return InvalidIngredient(error=error)
