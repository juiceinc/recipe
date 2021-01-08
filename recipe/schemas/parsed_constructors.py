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
    """ Create an ingredient from config version 2 object . """
    kind = ingr_dict.pop("kind", "metric")
    IngredientClass = ingredient_class_for_name(kind.title())
    if IngredientClass is None:
        raise BadIngredient(f"Unknown ingredient kind {kind}")

    args = []

    try:
        if kind in ("metric", "dimension"):
            if kind == "metric":
                fld_defn = ingr_dict.pop("field", None)
                # SQLAlchemy ingredient with required aggregation
                args = [builder.parse(fld_defn, enforce_aggregation=True, debug=debug)]
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
                args = [builder.parse(fld_defn, forbid_aggregation=True, debug=debug)]
                # Convert extra fields to sqlalchemy expressions and add them directly to
                # the kwargs
                for extra in ingr_dict.pop("extra_fields", []):
                    ingr_dict[extra.get("name")] = builder.parse(
                        extra.get("field"), forbid_aggregation=True, debug=debug
                    )

            # TODO: Restore quickselects
            # Convert quickselects to a kwarg with sqlalchemy expressions
            # parsed_quickselects = []
            # for qf in ingr_dict.pop("quickselects", []):
            #     parsed_quickselects.append(
            #         {
            #             "name": qf["name"],
            #             "condition": TransformToSQLAlchemyExpression(
            #                 selectable=selectable
            #             ).transform(noag_full_condition_parser.parse(qf.get("condition"))),
            #         }
            #     )
            # ingr_dict["quickselects"] = parsed_quickselects

        elif kind == "filter":
            pass
            # TODO: restore this
            # Create a sqlalchemy expression from 'condition' and pass it as the first arg
            # args = [
            #     TransformToSQLAlchemyExpression(selectable=selectable).transform(
            #         noag_full_condition_parser.parse(ingr_dict.pop("condition", None))
            #     )
            # ]

        elif kind == "having":
            pass
            # TODO: restore this
            # Create a sqlalchemy expression from 'condition' and pass it as the first arg
            # TODO: Force this to be an aggregate
            # args = [
            #     TransformToSQLAlchemyExpression(selectable=selectable).transform(
            #         full_condition_parser.parse(ingr_dict.pop("condition", None))
            #     )
            # ]
    except (GrammarError, LarkError) as e:
        error_msg = str(e)
        if "Expecting:" in error_msg:
            error_msg = error_msg.split("Expecting:")[0]

        error = {
            "type": "Can not parse field",
            "extra": {"details": error_msg,},
        }
        return InvalidIngredient(error=error)

    try:
        return IngredientClass(*args, **ingr_dict)
    except BadIngredient as e:
        # Some internal error while running the Ingredient constructor
        error = {
            "type": "bad_ingredient",
            "extra": {"details": str(e),},
        }
        return InvalidIngredient(error=error)
