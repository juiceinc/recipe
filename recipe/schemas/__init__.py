from sureberus import schema as S

from .config_schemas import _field_schema, _full_condition_schema, shelf_schema
from .parsed_schemas import shelf_schema as parsed_shelf_schema

shelf_schema = S.Dict(
    choose_schema=S.when_key_is(
        "_version",
        {
            "1": shelf_schema,
            1: shelf_schema,
            "2": parsed_shelf_schema,
            2: parsed_shelf_schema,
        },
        default_choice="1",
    )
)

# This schema is used with sureberus
recipe_schema = S.Dict(
    schema={
        "metrics": S.List(schema=S.String(), required=False),
        "dimensions": S.List(schema=S.String(), required=False),
        "filters": S.List(schema={"oneof": [S.String(), "condition"]}, required=False),
        "order_by": S.List(schema=S.String(), required=False),
    },
    registry={
        "aggregated_field": _field_schema(aggr=True, required=True),
        "optional_aggregated_field": _field_schema(aggr=True, required=False),
        "non_aggregated_field": _field_schema(aggr=False, required=True),
        "condition": _full_condition_schema(aggr=False, label_required=False),
        "labeled_condition": _full_condition_schema(aggr=False, label_required=True),
        "having_condition": _full_condition_schema(aggr=True, label_required=False),
    },
)
