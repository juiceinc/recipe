"""
Validators use cerberus to validate ingredients.yaml definitions
and convert them to


"""

import logging
from copy import deepcopy
from datetime import date, datetime

from cerberus import Validator, schema_registry
from cerberus.platform import _int_types, _str_type
from sqlalchemy import Float, Integer, String, case, distinct, func

logging.captureWarnings(True)

ingredient_schema = {
    'kind': {
        'type':
            'string',
        'required':
            True,
        'allowed': [
            'Ingredient',
            'Dimension',
            'LookupDimension',
            'IdValueDimension',
            'Metric',
            'DivideMetric',
            'WtdAvgMetric',
            'Filter',
            'Having',
        ],
        'default':
            'Metric'
    },
    'field': {
        'schema': 'field',
        'type': 'dict',
        'coerce': 'to_field_dict',
        'allow_unknown': False,
        'required': True
    },
    'format': {
        'type': 'string',
        'coerce': 'to_format_with_lookup'
    }
}

default_field_schema = {
    'value': {
        'type': 'string',
        'required': True,
    },
    '_aggregation': {
        'default_setter': 'aggregation',
        'nullable': True,
        'readonly': True
    },
    'aggregation': {
        'type':
            'string',
        'required':
            False,
        # Allowed values are the keys of IngredientValidator.aggregation_lookup
        'allowed': [
            'sum',
            'min',
            'max',
            'avg',
            'count',
            'count_distinct',
            'month',
            'week',
            'year',
            'quarter',
            'age',
            'none',
            None,
        ],
        'nullable':
            True,
        'default':
            None,
    },
    'condition': {
        'schema': 'condition',
        'contains_oneof': ['in', 'gt', 'gte', 'lt', 'lte', 'eq', 'ne'],
        'required': False,
        'allow_unknown': False
    }
}

field_schema = deepcopy(default_field_schema)

aggregated_field_schema = deepcopy(default_field_schema)
aggregated_field_schema['aggregation']['required'] = True
aggregated_field_schema['aggregation']['nullable'] = False
aggregated_field_schema['aggregation']['coerce'] = 'to_aggregation_with_default'

condition_schema = {
    'field': {
        'schema': 'field',
        'type': 'dict',
        'coerce': 'to_field_dict',
        'allow_unknown': False,
        'required': True
    },
    '_condition': {
        'default_setter': 'condition',
        'nullable': True,
        'readonly': True
    },
    'in': {
        'required': False,
        'type': 'list',
        'coerce': 'to_list'
    },
    'gt': {
        'required': False,
        'type': 'scalar'
    },
    'gte': {
        'required': False,
        'type': 'scalar'
    },
    'lt': {
        'required': False,
        'type': 'scalar'
    },
    'lte': {
        'required': False,
        'type': 'scalar'
    },
    'eq': {
        'required': False,
        'type': 'scalar'
    },
    'ne': {
        'required': False,
        'type': 'scalar'
    }
}

schema_registry.add('field', field_schema)
schema_registry.add('aggregated_field', aggregated_field_schema)
schema_registry.add('condition', condition_schema)
schema_registry.add('ingredient', ingredient_schema)


class IngredientValidator(Validator):
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

    aggregation_lookup = {
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

    condition_lookup = {
        'in': lambda fld: getattr(fld, 'in_'),
        'gt': lambda fld: getattr(fld, '__gt__'),
        'gte': lambda fld: getattr(fld, '__ge__'),
        'lt': lambda fld: getattr(fld, '__lt__'),
        'lte': lambda fld: getattr(fld, '__le__'),
        'eq': lambda fld: getattr(fld, '__eq__'),
        'ne': lambda fld: getattr(fld, '__ne__'),
    }

    default_aggregation = 'sum'

    def __init__(self, *args, **kwargs):
        # Set defaults
        kwargs['schema'] = kwargs.get('schema', ingredient_schema)
        kwargs['allow_unknown'] = kwargs.get('allow_unknown', True)
        kwargs['normalize'] = kwargs.get('normalize', True)
        super(IngredientValidator, self).__init__(*args, **kwargs)

    def _normalize_coerce_to_format_with_lookup(self, v):
        """ Replace a format with a default """
        try:
            return self.format_lookup.get(v, v)
        except TypeError:
            # v is something we can't lookup (like a list)
            return v

    def _normalize_coerce_to_aggregation_with_default(self, v):
        if v is None:
            return self.default_aggregation
        else:
            return v

    def _normalize_coerce_to_field_dict(self, v):
        """ coerces strings to a dict {'value': str} """
        if isinstance(v, _str_type):
            return {'value': v}
        else:
            return v

    def _normalize_coerce_to_list(self, v):
        if self._validate_type_scalar(v):
            return [v]
        else:
            return v

    def _validate_type_scalar(self, value):
        """ Is not a list or a dict """
        if isinstance(
            value, _int_types + (_str_type, float, date, datetime, bool)
        ):
            return True

    def _validate_contains_oneof(self, keys, field, value):
        """ Validates that exactly one of the keys exists in value """
        results = [k for k in keys if k in value]

        if len(results) == 0:
            self._error(field, 'Must contain one of {}'.format(keys))
            return False
        elif len(results) > 1:
            self._error(field, 'Must contain only one of {}'.format(keys))
            return False
        return True

    def _normalize_default_setter_condition(self, document):
        for k in self.condition_lookup.keys():
            if k in document:
                return self.condition_lookup[k]
        return None

    def _normalize_default_setter_aggregation(self, document):
        aggr = document.get('aggregation', None)
        return self.aggregation_lookup.get(aggr, None)
