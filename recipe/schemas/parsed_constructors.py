"""Convert parsed trees into SQLAlchemy objects """
from datetime import date
from .builders import SQLAlchemyBuilder
from lark.exceptions import GrammarError, LarkError

from recipe.exceptions import BadIngredient
from recipe.ingredients import InvalidIngredient

from .utils import ingredient_class_for_name


def _convert_bucket_to_field(field, bucket, buckets_default_label, builder):
    """Convert a bucket structure if statement

    This assumes that all conditions have been converted from
    partial conditions like '<5' to full conditions like 'age<5'
    with _convert_partial_conditions
    """

    def stringify(value):
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

    parts, order_by_parts, idx = [], [], 0
    for itm in bucket:
        cond = itm.get("condition")
        label = itm.get("label")
        # Determine if the field needs to be prepended to the condition """"
        try:
            # The expression may not contain aggregations
            builder.parse(cond, forbid_aggregation=True)
        except Exception:
            cond = f"{field} {cond}"
            # Ensure this is parsable
            builder.parse(cond, forbid_aggregation=True)
        parts.append(cond)
        order_by_parts.append(cond)
        parts.append(stringify(label))
        order_by_parts.append(str(idx))
        idx += 1

    # Add the default value
    if buckets_default_label is None:
        buckets_default_label = "Not found"
    # Add a final default label
    parts.append(stringify(buckets_default_label))
    order_by_parts.append(str(9999))

    return "if(" + ",".join(parts) + ")", "if(" + ",".join(order_by_parts) + ")"


def get_convert_dates(format: str):
    """If a format only shows years or months, wrap dates in a conversion in the builder"""
    if not isinstance(format, str):
        return
    if format == "%Y":
        return "year_conv"
    # A format for months but not days
    elif (
        "%B" in format and "%Y" in format and "%-d" not in format and "%d" not in format
    ):
        return "month_conv"


def get_convert_datetimes(format: str):
    """If a format only shows years or months, wrap datetimes in a conversion in the builder"""
    if not isinstance(format, str):
        return
    if format == "%Y":
        return "dt_year_conv"
    # A format for months but not days
    elif (
        "%B" in format and "%Y" in format and "%-d" not in format and "%d" not in format
    ):
        return "dt_month_conv"
    elif "%H" not in format:
        return "dt_day_conv"


def convert_quickselects(
    builder: SQLAlchemyBuilder, ingr_dict: dict, builder_kwargs: dict
):
    """Convert quickselects from expressions to"""
    if "quickselects" in ingr_dict:
        parsed_quickselects = []
        for qs in ingr_dict.pop("quickselects", []):
            condition_defn = qs.get("condition")
            expr, _ = builder.parse(
                condition_defn, forbid_aggregation=True, **builder_kwargs
            )
            parsed_quickselects.append({"name": qs["name"], "condition": expr})
        ingr_dict["quickselects"] = parsed_quickselects


def convert_filter(builder: SQLAlchemyBuilder, ingr_dict: dict, builder_kwargs: dict):
    """If a filter property exists, validate that it is a boolean expression and add it
    to the ingredient filters"""
    if "filter" in ingr_dict:
        filt_expression = ingr_dict.pop("filter")

        expr, datatype = builder.parse(
            filt_expression, forbid_aggregation=True, **builder_kwargs
        )
        if datatype == "boolean":
            ingr_dict["filters"] = [expr]


def convert_buckets_to_field_defn(
    builder: SQLAlchemyBuilder, ingr_dict: dict, fld_defn: str, builder_kwargs: dict
):
    """If a buckets key exists, convert it to an expression, add
    the order_by to extra_fields.
    """
    buckets = ingr_dict.pop("buckets", None)
    buckets_default_label = ingr_dict.pop("buckets_default_label", None)
    if buckets:
        fld_defn, order_by_fld = _convert_bucket_to_field(
            fld_defn, buckets, buckets_default_label, builder
        )
        if "extra_fields" not in ingr_dict:
            ingr_dict["extra_fields"] = []
        ingr_dict["extra_fields"].append(
            {"name": "order_by_expression", "field": order_by_fld}
        )
    return fld_defn


def convert_extra_fields(
    builder: SQLAlchemyBuilder, ingr_dict: dict, builder_kwargs: dict
):
    """
    Dimensions may contain extra expressions in the extra_fields list.

    Convert extra fields to sqlalchemy expressions and add them directly to
    the kwargs, saving datatypes in datatype_by_role
    """
    for extra_fld in ingr_dict.pop("extra_fields", []):
        # Extra_fields will be dicts with two keys
        # name and field
        raw_role = extra_fld.get("name")
        role = raw_role.rstrip("_expression")

        expr, datatype = builder.parse(
            extra_fld.get("field"), forbid_aggregation=True, **builder_kwargs
        )
        ingr_dict["datatype_by_role"]["role"] = datatype
        ingr_dict[raw_role] = expr


def create_ingredient_from_parsed(
    ingr_dict: dict, builder: SQLAlchemyBuilder, debug: bool = False
):
    """Create an ingredient from config version 2 object ."""
    kind = ingr_dict.pop("kind", "metric")
    IngredientClass = ingredient_class_for_name(kind.title())
    if IngredientClass is None:
        raise BadIngredient(f"Unknown ingredient kind {kind}")

    args = []

    clean_format = ingr_dict.get("format")
    if (
        isinstance(clean_format, str)
        and clean_format.startswith("<")
        and clean_format.endswith(">")
    ):
        clean_format = clean_format[1:-1]

    # For some formats, we will automatically convert dates to year or month in the builder
    builder_kwargs = {
        "debug": debug,
        "convert_dates_with": get_convert_dates(clean_format),
        "convert_datetimes_with": get_convert_datetimes(clean_format),
    }

    if builder.drivername.startswith("mssql") or builder.drivername.startswith(
        "snowflake"
    ):
        # SQLServer can not use aliases in group bys and also
        # does not support date/time conversions due to an issue with pyodbc
        # parameters in queries
        # https://github.com/mkleehammer/pyodbc/issues/479
        default_group_by_strategy = "direct"
    else:
        default_group_by_strategy = "labels"

    try:
        if kind == "metric":
            ingr_dict["group_by_strategy"] = ingr_dict.get(
                "group_by_strategy", default_group_by_strategy
            )

            fld_defn = ingr_dict.pop("field", None)
            # SQLAlchemy ingredient with required aggregation
            expr, datatype = builder.parse(
                fld_defn, enforce_aggregation=True, **builder_kwargs
            )
            # Save the data type in the ingredient
            ingr_dict["datatype"] = datatype
            if datatype != "num":
                error = {
                    "type": "Can not parse field",
                    "extra": {"details": "A string can not be aggregated"},
                }
                return InvalidIngredient(error=error)

            convert_filter(builder, ingr_dict, builder_kwargs)
            convert_quickselects(builder, ingr_dict, builder_kwargs)
            args = [expr]

        elif kind == "dimension":
            fld_defn = ingr_dict.pop("field", None)
            fld_defn = convert_buckets_to_field_defn(
                builder, ingr_dict, fld_defn, builder_kwargs
            )

            expr, datatype = builder.parse(
                fld_defn, forbid_aggregation=True, **builder_kwargs
            )
            # Save the data type in the ingredient
            ingr_dict["datatype"] = datatype
            args = [expr]
            ingr_dict["datatype_by_role"] = {"value": datatype}

            convert_extra_fields(builder, ingr_dict, builder_kwargs)
            convert_filter(builder, ingr_dict, builder_kwargs)
            convert_quickselects(builder, ingr_dict, builder_kwargs)

        elif kind == "filter":
            condition_defn = ingr_dict.get("condition")
            expr, datatype = builder.parse(
                condition_defn, forbid_aggregation=True, **builder_kwargs
            )
            args = [expr]

        elif kind == "having":
            condition_defn = ingr_dict.get("condition")
            expr, datatype = builder.parse(
                condition_defn, forbid_aggregation=False, **builder_kwargs
            )
            args = [expr]

    except (GrammarError, LarkError) as e:
        error_msg = str(e)
        if "Expecting:" in error_msg:
            error_msg = error_msg.split("Expecting:")[0]

        error = {"type": "Can not parse field", "extra": {"details": error_msg}}
        return InvalidIngredient(error=error)

    try:
        return IngredientClass(*args, **ingr_dict)
    except BadIngredient as e:
        # Some internal error while running the Ingredient constructor
        error = {"type": "bad_ingredient", "extra": {"details": str(e)}}
        return InvalidIngredient(error=error)
