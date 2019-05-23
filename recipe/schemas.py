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

from recipe.compat import basestring

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


# This schema is used with sureberus
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

format_lookup = {
    'comma': ',.0f',
    'dollar': '$,.0f',
    'percent': '.0%',
    'comma1': ',.1f',
    'dollar1': '$,.1f',
    'percent1': '.1%',
    'comma2': ',.2f',
    'dollar2': '$,.2f',
    'percent2': '.2%',
}

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
field_pattern = re.compile(r'^({})\((.*)\)$'.format(aggr_keys))


def find_operators(value):
    """ Find operators in a field that may look like "a+b-c" """
    field = re.split('[+-/*]', value)[0]

    operator_lookup = {'-': 'sub', '+': 'add', '/': 'div', '*': 'mul'}

    operators = []
    for part in re.findall('[+-/*]\w+', value):
        # TODO: Full validation on other fields
        other_field = _coerce_string_into_field(part[1:])
        operators.append({
            'operator': operator_lookup[part[0]],
            'field': other_field
        })
    return field, operators


def _coerce_string_into_field(value):
    """ Convert a string into a field, potentially parsing a functional
    form into a value and aggregation """
    if isinstance(value, basestring):
        # Remove all whitespace
        value = re.sub(r'\s+', '', value, flags=re.UNICODE)
        m = re.match(field_pattern, value)
        if m:
            aggr, value = m.groups()
            value, operators = find_operators(value)
            result = {'value': value, 'aggregation': aggr}
            if operators:
                result['operators'] = operators
            return result
        else:
            value, operators = find_operators(value)
            result = {'value': value}
            if operators:
                result['operators'] = operators
            return result
    else:
        return value


def _inject_aggregation_fn(field):
    field['_aggregation_fn'] = aggregations.get(field['aggregation'])
    return field


def _field_schema(aggregate=True, use_registry=False):
    """Make a field schema that either aggregates or doesn't. """
    if aggregate:
        aggr = S.String(
            required=False,
            allowed=list(aggregations.keys()),
            default=default_aggregation,
            nullable=True
        )
    else:
        aggr = S.String(
            required=False,
            allowed=[no_aggregation, None],
            default=no_aggregation,
            nullable=True
        )

    if use_registry:
        condition = 'condition'
    else:
        condition = S.Dict(required=False)

    operator = S.Dict({
        'operator': S.String(allowed=['add', 'sub', 'div', 'mul']),
        'field': S.String()
    })

    field_schema = S.Dict(
        schema={
            'value':
                S.String(),
            'aggregation':
                aggr,
            'condition':
                condition,
            'format':
                S.String(
                    coerce=lambda v: format_lookup.get(v, v), required=False
                ),
            'operators':
                S.List(schema=operator, required=False)
        },
        coerce=_coerce_string_into_field,
        coerce_post=_inject_aggregation_fn,
        allow_unknown=False,
        required=True,
    )
    return field_schema


aggregated_field_schema = _field_schema(aggregate=True)
non_aggregated_field_schema = _field_schema(aggregate=False)

metric_schema = S.Dict(
    allow_unknown=True, schema={
        'field': _field_schema(aggregate=True)
    }
)
dimension_schema = S.Dict(
    allow_unknown=True, schema={
        'field': _field_schema(aggregate=False)
    }
)


class ConditionPost(object):
    """ Convert an operator like 'gt', 'lt' into '_op' and '_op_value'
    for easier parsing into SQLAlchemy """

    def __init__(self, operator, _op):
        self.operator = operator
        self._op = _op

    def __call__(self, value):
        value['_op'] = self._op
        value['_op_value'] = value.get(self.operator)
        value.pop(self.operator)
        return value


def _condition_schema(operator, _op, scalar=True, use_registry=False):
    if scalar:
        allowed_values = [S.Integer(), S.String(), S.Float(), S.Boolean()]
    else:
        allowed_values = [S.List()]

    if use_registry:
        field = 'non_aggregated_field'
    else:
        field = non_aggregated_field_schema

    _condition_schema = S.Dict(
        allow_unknown=False,
        schema={'field': field,
                operator: {
                    'anyof': allowed_values
                }},
        coerce_post=ConditionPost(operator, _op)
    )
    return _condition_schema


def _full_condition_schema(use_registry=False):
    """ Conditions can be a field with an operator, like this yaml example

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
    """

    # Handle conditions where there's an operator
    operator_condition = S.DictWhenKeyExists({
        'gt':
            _condition_schema('gt', '__gt__', use_registry=use_registry),
        'gte':
            _condition_schema('gte', '__ge__', use_registry=use_registry),
        'ge':
            _condition_schema('ge', '__ge__', use_registry=use_registry),
        'lt':
            _condition_schema('lt', '__lt__', use_registry=use_registry),
        'lte':
            _condition_schema('lte', '__le__', use_registry=use_registry),
        'le':
            _condition_schema('le', '__le__', use_registry=use_registry),
        'eq':
            _condition_schema('eq', '__eq__', use_registry=use_registry),
        'ne':
            _condition_schema('ne', '__ne__', use_registry=use_registry),
        'in':
            _condition_schema(
                'in', 'in_', scalar=False, use_registry=use_registry
            ),
        'notin':
            _condition_schema(
                'notin', 'notin', scalar=False, use_registry=use_registry
            ),
    },
                                             required=False,
                                             allow_unknown=False)

    # A list of or-ed conditions
    or_list_condition = S.Dict(
        schema={'or': S.List(schema=operator_condition)}, allow_unknown=False
    )

    # A list of and-ed conditions
    and_list_condition = S.Dict(
        schema={'and': S.List(schema=operator_condition)}, allow_unknown=False
    )

    return S.Dict(
        anyof=[operator_condition, or_list_condition, and_list_condition],
        required=False,
        allow_unknown=False
    )


condition_schema = _full_condition_schema(use_registry=False)

# Create a full schema that uses a registry
ingredient_schema = S.DictWhenKeyIs(
    'kind', {
        'Metric':
            S.Dict(
                allow_unknown=True,
                schema={
                    'field': _field_schema(aggregate=True, use_registry=True)
                }
            ),
        'Dimension':
            S.Dict(
                allow_unknown=True,
                schema={
                    'field': _field_schema(aggregate=False, use_registry=True)
                }
            )
    },
    registry={
        'aggregated_field':
            _field_schema(aggregate=True, use_registry=True),
        'non_aggregated_field':
            _field_schema(aggregate=False, use_registry=True),
        'condition':
            _full_condition_schema(use_registry=True),
    }
)

shelf_schema = S.Dict(
    valueschema=ingredient_schema, keyschema=S.String(), allow_unknown=True
)
