"""
Registers recipe schemas
"""

import logging
import re
from collections import OrderedDict
from copy import deepcopy

from cerberus import schema_registry
from sqlalchemy import distinct, func
from sureberus import schema as S

logging.captureWarnings(True)

IdValueDimensionFields = OrderedDict()


class RecipeSchemas(object):

    def __init__(
        self, allowed_aggregations, allowed_operators=['+', '-', '*', '/']
    ):
        self.allowed_aggregations = allowed_aggregations
        self.allowed_operators = allowed_operators

        self.scalar_conditions = ('gt', 'gte', 'lt', 'lte', 'eq', 'ne')
        self.nonscalar_conditions = ('in',)

        # A structure containing ingredient classes and the required
        # parameters IN THE ORDER that they are passed to the class
        # constructor.
        self.ingredient_kinds = {
            'Ingredient': {
                'field': 'field'
            },
            'Dimension': {
                'field': 'field'
            },
            'LookupDimension': {
                'field': 'field'
            },
            'IdValueDimension':
                OrderedDict([('id_field', 'field'), ('field', 'field')]),
            'Metric': {
                'field': 'aggregated_field'
            },
            'DivideMetric':
                OrderedDict([('numerator_field', 'aggregated_field'),
                             ('denominator_field', 'aggregated_field')]),
            'WtdAvgMetric':
                OrderedDict([
                    ('field', 'field'),
                    ('weight', 'field'),
                ]),
            # FIXME: what to do about these guys, field isn't the right
            # SQLAlchemy structure
            'Filter': {
                'field': 'field'
            },
            'Having': {
                'field': 'field'
            },
        }
        self.register_schemas()

    def _register_field_schemas(self):
        default_field_schema = {
            'value': {
                'type': 'string',
                'required': True,
            },
            'operators': {
                'required': False,
                'type': 'list',
                'schema': {
                    'schema': 'operator',
                }
            },
            'aggregation': {
                'type': 'string',
                'required': False,
                # Allowed values are the keys of
                # IngredientValidator.aggregation_lookup
                'allowed': self.allowed_aggregations,
                'nullable': True,
                'default': None,
            },
            'condition': {
                'schema': 'condition',
                'validator': self._validate_condition_keys,
                'required': False,
                'allow_unknown': False,
                'type': 'dict'
            }
        }

        # Aggregated fields coerce null values to the default aggregation
        aggregated_field_schema = deepcopy(default_field_schema)
        aggregated_field_schema['aggregation']['required'] = True
        aggregated_field_schema['aggregation']['nullable'] = False
        aggregated_field_schema['aggregation']['coerce'] = \
            'to_aggregation_with_default'

        schema_registry.add('field', deepcopy(default_field_schema))
        schema_registry.add('aggregated_field', aggregated_field_schema)

    def _register_operator_schema(self):
        operator_schema = {
            'operator': {
                'type': 'string',
                'allowed': self.allowed_operators,
                'required': True
            },
            'field': {
                'schema': 'field',
                'type': 'dict',
                'coerce': 'to_field_dict',
                'allow_unknown': False,
                'required': True
            },
        }

        schema_registry.add('operator', operator_schema)

    def _validate_condition_keys(self, field, value, error):
        """
        Validates that all of the keys in one of the sets of keys are defined
        as keys of ``value``.
        """
        if 'field' in value:
            operators = self.nonscalar_conditions + self.scalar_conditions
            matches = sum(1 for k in operators if k in value)
            if matches == 0:
                error(field, 'Must contain one of {}'.format(operators))
                return False
            elif matches > 1:
                error(
                    field,
                    'Must contain no more than one of {}'.format(operators)
                )
                return False
            return True
        elif 'and' in value:
            for condition in value['and']:
                self._validate_condition_keys(field, condition, error)
        elif 'or' in value:
            for condition in value['or']:
                self._validate_condition_keys(field, condition, error)
        else:
            error(field, "Must contain field + operator keys, 'and', or 'or'.")
            return False

    def _register_condition_schema(self):
        recursive = {
            'type': 'dict',
            'schema': 'condition',
            'validator': self._validate_condition_keys
        }
        condition_schema = {
            'and': {
                'type': 'list',
                'schema': recursive,
            },
            'or': {
                'type': 'list',
                'schema': recursive,
            },
            'field': {
                'schema': 'field',
                'type': 'dict',
                'coerce': 'to_field_dict',
                'allow_unknown': False,
                'required': False,
            },
        }

        for nonscalar_cond in self.nonscalar_conditions:
            condition_schema[nonscalar_cond] = {
                'required': False,
                'type': 'list',
                'coerce': 'to_list'
            }
        for scalar_cond in self.scalar_conditions:
            condition_schema[scalar_cond] = {
                'required': False,
                'type': 'scalar'
            }

        schema_registry.add('condition', condition_schema)

    def _register_ingredient_schemas(self):
        ingredient_schema_root = {
            'kind': {
                'type': 'string',
                'required': True,
                'allowed': list(self.ingredient_kinds.keys()),
                'default': 'Metric'
            },
            '_fields': {
                'nullable': True,
                'readonly': True,
                'type': 'list',
                'default': [],
            },
            'format': {
                'type': 'string',
                'coerce': 'to_format_with_lookup'
            }
        }

        for kind, extras in self.ingredient_kinds.items():
            # Build a schema for each kind of ingredient
            schema = deepcopy(ingredient_schema_root)
            schema['_fields']['default'] = list(extras.keys())
            for field_name, field_schema in extras.items():
                schema[field_name] = {
                    'schema': field_schema,
                    'type': 'dict',
                    'coerce': 'to_field_dict',
                    'allow_unknown': False,
                    'required': True
                }
            schema_registry.add(kind, schema)

    def register_schemas(self):
        self._register_field_schemas()
        self._register_operator_schema()
        self._register_condition_schema()
        self._register_ingredient_schemas()


def _coerce_filter(v):
    # For now, we'll delegate to the validator / normalizer using
    # Cerberus.
    from recipe.validators import IngredientValidator
    validator = IngredientValidator(schema='Filter')
    if not validator.validate(v):
        raise Exception(validator.errors)
    validator.document['kind'] = 'Filter'
    return validator.document


recipe_schema = {
    'type': 'dict',
    'schema': {
        # These directives correspond with the keyword arguments of Recipe
        # class.
        'metrics': {
            'type': 'list',
            'schema': {
                'type': 'string'
            },
        },
        'dimensions': {
            'type': 'list',
            'schema': {
                'type': 'string'
            },
        },
        'filters': {
            'type': 'list',
            'schema': {
                'oneof': [
                    {
                        'type': 'string'
                    },
                    {
                        'type': 'dict',
                        'coerce': _coerce_filter,
                    },
                ]
            },
        },
        'order_by': {
            'type': 'list',
            'schema': {
                'type': 'string'
            },
        },
    }
}

condition_schema = {}
# """
# state:
#     kind: LookupDimension
#     field: state
#     lookup:
#         Vermont: "The Green Mountain State"
#         Tennessee: "The Volunteer State"
# pop2000:
#     kind: Metric
#     field:
#         value: pop2000
#         condition:
#             field: age
#             gt: 40
# allthemath:
#     kind: Metric
#     field: pop2000+pop2008   - pop2000 * pop2008 /pop2000
# """

default_aggregation = 'sum'
no_aggregation = 'none'
aggregations = {
    'sum': func.sum,
    'min': func.min,
    'max': func.max,
    'avg': func.avg,
    'count': func.count,
    'count_distinct': lambda fld: func.count(distinct(fld)),
    'month': lambda fld: func.date_trunc('month', fld),
    'week': lambda fld: func.date_trunc('week', fld),
    'year': lambda fld: func.date_trunc('year', fld),
    'quarter': lambda fld: func.date_trunc('quarter', fld),
    'age': lambda fld: func.date_part('year', func.age(fld)),
    'none': lambda fld: fld,
    None: lambda fld: fld,
}

aggr_keys = '|'.join(
    k for k in aggregations.keys() if isinstance(k, basestring)
)
# Match patterns like sum(a)
field_pattern = re.compile('^({})\((.*)\)$'.format(aggr_keys))


def _coerce_string_into_field(value):
    """ Convert a string into a field """
    if isinstance(value, basestring):
        value = value.strip()
        m = re.match(field_pattern, value)
        if m:
            aggr, value = m.groups()
            return {'value': value, 'aggregation': aggr}
        else:
            return {'value': value}
    else:
        return value


def coerce_aggregations(doc):
    doc['_aggregation_fn'] = aggregations.get(doc['aggregation'])
    return doc


non_aggregation = S.String(
    required=False,
    allowed=[no_aggregation, None],
    default=no_aggregation,
    nullable=True
)
aggregation = S.String(
    required=False,
    allowed=list(aggregations.keys()),
    default=default_aggregation,
    nullable=True
)


def _field_schema(aggregation_strategy):
    """Make a field schema that either aggregates or doesn't. """
    return S.Dict(
        schema={
            'value': S.String(),
            'aggregation': aggregation_strategy,
            'condition': S.Dict(required=False)
        },
        coerce=_coerce_string_into_field,
        coerce_post=coerce_aggregations,
        allow_unknown=False,
        required=True,
    )


aggregated_field_schema = _field_schema(aggregation)
non_aggregated_field_schema = _field_schema(non_aggregation)
metric_schema = S.Dict(
    allow_unknown=True, schema={
        'field': aggregated_field_schema
    }
)
dimension_schema = S.Dict(
    allow_unknown=True, schema={
        'field': non_aggregated_field_schema
    }
)

ingredient_schema = S.DictWhenKeyIs(
    'kind', {
        'Metric': metric_schema,
        'Dimension': dimension_schema
    }
)


def _condition_schema():
    return {
        # 'registry': {
        #     'condition': {
        #         'type': 'dict',
        #         'schema': {
        #             'oneof': [
        #                 'and': {
        #                     'type': 'list',
        #                     'schema_ref': 'condition'
        #                 },
        #                 'or': {
        #                     'type': 'list',
        #                     'schema_ref': 'condition'
        #                 },
        #                 'field':  _field_schema()
        #             ]
        #         }
        #     }
        # },
        'type': 'dict',
        'schema': _field_schema,
        # 'schema_ref': 'condition'
    }


condition_schema = _condition_schema()

# Operators are an option for fields
# """
# # sum(mytable.foo+mytable.bar)
# op:
#     field:
#         add:
#         - value: foo
#         - value: bar
#         aggregation: sum
# # max(mytable.foo - mytable.bar)
# op2:
#     field:
#         sub:
#         - value: foo
#         - value: bar
#         aggregation: max
# # naive divide, see divide_by for divide w strategy
# # sum((mytable.x+mytable.y+mytable.z)/(mytable.xx-mytable.yy))
# # sub,div,mul are binary operators, add is multi
# op3:
#     field:
#         div:
#             - add:
#                 - x
#                 - y
#                 - x
#             - sub:
#                 - xx
#                 - yy
# # naive weighted avg expressed w operators
# op4:
#     field:
#         div:
#             - mul:
#                 - expr
#                 - wt
#             - wt
# # Anywhere a field is displayed
# """


def _operator_schema():
    return {
        'registry': {
            'nested_list': {
                'type': 'list',
                'schema': {
                    'anyof': [
                        {
                            'type': 'string'
                        },
                        'nested_list',
                    ],
                }
            }
        },
        'type': 'dict',
        'schema': {
            'things': 'nested_list'
        },
    }
