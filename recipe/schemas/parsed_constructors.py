"""Convert parsed trees into SQLAlchemy objects """
from datetime import date

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
        except:
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


def create_ingredient_from_parsed(ingr_dict, builder, debug=False):
    """Create an ingredient from config version 2 object ."""
    kind = ingr_dict.pop("kind", "metric")
    IngredientClass = ingredient_class_for_name(kind.title())
    if IngredientClass is None:
        raise BadIngredient(f"Unknown ingredient kind {kind}")

    args = []

    # For some formats, we will automatically convert dates
    format = ingr_dict.get("format")
    if isinstance(format, str) and format.startswith("<") and format.endswith(">"):
        format = format[1:-1]
    convert_dates_lookup = {"%Y": "year_conv", "%B %Y": "month_conv"}
    convert_dates_with = convert_dates_lookup.get(format)
    convert_datetimes_lookup = {
        "%Y": "dt_year_conv",
        "%B %Y": "dt_month_conv",
        "%B %-d, %Y": "dt_day_conv",
        "%-d %b %Y": "dt_day_conv",
        "%-m/%-d/%Y": "dt_day_conv",
        "%B %-d, %Y": "dt_day_conv",
        "%-m-%-d-%Y": "dt_day_conv",
    }
    convert_datetimes_with = convert_datetimes_lookup.get(format)

    try:
        if kind in ("metric", "dimension"):
            if kind == "metric":
                fld_defn = ingr_dict.pop("field", None)
                # SQLAlchemy ingredient with required aggregation
                expr, datatype = builder.parse(
                    fld_defn,
                    enforce_aggregation=True,
                    debug=debug,
                    convert_dates_with=convert_dates_with,
                    convert_datetimes_with=convert_datetimes_with,
                )
                # Save the data type in the ingredient
                ingr_dict["datatype"] = datatype
                if datatype != "num":
                    error = {
                        "type": "Can not parse field",
                        "extra": {"details": "A string can not be aggregated"},
                    }
                    return InvalidIngredient(error=error)
                args = [expr]
            else:
                fld_defn = ingr_dict.pop("field", None)
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
                expr, datatype = builder.parse(
                    fld_defn,
                    forbid_aggregation=True,
                    debug=debug,
                    convert_dates_with=convert_dates_with,
                    convert_datetimes_with=convert_datetimes_with,
                )
                # Save the data type in the ingredient
                ingr_dict["datatype"] = datatype
                args = [expr]

                # Convert extra fields to sqlalchemy expressions and add them directly to
                # the kwargs, saving datatypes
                datatype_by_role = {"value": datatype}
                for extra in ingr_dict.pop("extra_fields", []):
                    raw_role = extra.get("name")
                    if raw_role.endswith("_expression"):
                        # Remove _expression to get the role
                        role = raw_role[:-11]
                    else:
                        role = raw_role

                    expr, datatype = builder.parse(
                        extra.get("field"),
                        forbid_aggregation=True,
                        debug=debug,
                        convert_dates_with=convert_dates_with,
                        convert_datetimes_with=convert_datetimes_with,
                    )
                    datatype_by_role[role] = datatype
                    ingr_dict[raw_role] = expr
                ingr_dict["datatype_by_role"] = datatype_by_role

            parsed_quickselects = []
            for qs in ingr_dict.pop("quickselects", []):
                condition_defn = qs.get("condition")
                expr, _ = builder.parse(
                    condition_defn,
                    forbid_aggregation=True,
                    debug=debug,
                    convert_dates_with=convert_dates_with,
                    convert_datetimes_with=convert_datetimes_with,
                )
                parsed_quickselects.append({"name": qs["name"], "condition": expr})
            ingr_dict["quickselects"] = parsed_quickselects

        elif kind == "filter":
            condition_defn = ingr_dict.get("condition")
            expr, _ = builder.parse(
                condition_defn,
                forbid_aggregation=True,
                debug=debug,
                convert_dates_with=convert_dates_with,
                convert_datetimes_with=convert_datetimes_with,
            )
            args = [expr]
        elif kind == "having":
            condition_defn = ingr_dict.get("condition")
            expr, _ = builder.parse(
                condition_defn,
                forbid_aggregation=False,
                debug=debug,
                convert_dates_with=convert_dates_with,
                convert_datetimes_with=convert_datetimes_with,
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
