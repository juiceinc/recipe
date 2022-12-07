from sureberus import schema as S
from .parsed_schemas import shelf_schema

# This schema is used with sureberus
recipe_schema = S.Dict(
    schema={
        "metrics": S.List(schema=S.String(), required=False),
        "dimensions": S.List(schema=S.String(), required=False),
        "filters": S.List(schema=S.String(), required=False),
        "order_by": S.List(schema=S.String(), required=False),
    }
)
