"""
Registers recipe schemas
"""

import logging
import re
from collections import OrderedDict
from copy import deepcopy

from sqlalchemy import distinct, func
from sureberus import schema as S

from recipe.compat import basestring

logging.captureWarnings(True)

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


def _field_schema(aggregate=True):
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

    operator = S.Dict({
        'operator': S.String(allowed=['add', 'sub', 'div', 'mul']),
        'field': S.String()
    })

    return S.Dict(
        schema={
            'value': S.String(),
            'aggregation': aggr,
            'condition': 'condition',
            'operators': S.List(schema=operator, required=False)
        },
        coerce=_coerce_string_into_field,
        coerce_post=_inject_aggregation_fn,
        allow_unknown=False,
        required=True,
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


def _condition_schema(operator, _op, scalar=True):
    if scalar:
        allowed_values = [S.Integer(), S.String(), S.Float(), S.Boolean()]
    else:
        allowed_values = [S.List()]

    _condition_schema = S.Dict(
        allow_unknown=False,
        schema={
            'field': 'non_aggregated_field',
            operator: {
                'anyof': allowed_values
            }
        },
        coerce_post=ConditionPost(operator, _op)
    )
    return _condition_schema


def _full_condition_schema():
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
        'gt': _condition_schema('gt', '__gt__'),
        'gte': _condition_schema('gte', '__ge__'),
        'ge': _condition_schema('ge', '__ge__'),
        'lt': _condition_schema('lt', '__lt__'),
        'lte': _condition_schema('lte', '__le__'),
        'le': _condition_schema('le', '__le__'),
        'eq': _condition_schema('eq', '__eq__'),
        'ne': _condition_schema('ne', '__ne__'),
        'in': _condition_schema('in', 'in_', scalar=False),
        'notin': _condition_schema('notin', 'notin', scalar=False),
        'or': S.Dict(schema={
            'or': S.List(schema='condition')
        }),
        'and': S.Dict(schema={
            'and': S.List(schema='condition')
        }),
    },
                                             required=False)

    return {
        'registry': {
            'condition': operator_condition
        },
        'schema_ref': 'condition',
    }


# Create a full schema that uses a registry
ingredient_schema = S.DictWhenKeyIs(
    'kind', {
        'Metric':
            S.Dict(
                allow_unknown=True,
                schema={
                    'field':
                        'aggregated_field',
                    'format':
                        S.String(
                            coerce=lambda v: format_lookup.get(v, v),
                            required=False
                        )
                }
            ),
        'Dimension':
            S.Dict(
                allow_unknown=True,
                schema={
                    'field':
                        'non_aggregated_field',
                    'format':
                        S.String(
                            coerce=lambda v: format_lookup.get(v, v),
                            required=False
                        )
                },
            )
    },
    registry={
        'aggregated_field': _field_schema(aggregate=True),
        'non_aggregated_field': _field_schema(aggregate=False),
        'condition': _full_condition_schema(),
    }
)

shelf_schema = S.Dict(
    valueschema=ingredient_schema, keyschema=S.String(), allow_unknown=True
)
