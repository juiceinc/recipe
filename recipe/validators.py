import logging
from datetime import date, datetime

from cerberus import Validator, schema_registry
from cerberus.platform import _int_types, _str_type
from sqlalchemy import Float, Integer, String, case, distinct, func

logging.captureWarnings(True)


class IngredientValidator(Validator):

    def __init__(self, *args, **kwargs):
        super(IngredientValidator, self).__init__(*args, **kwargs)

        self.format_lookup = {
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
        self.aggregation_lookup = {
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
            None: lambda fld: fld,
        }
        self.condition_lookup = {
            'in': lambda fld: getattr(fld, 'in_'),
            'gt': lambda fld: getattr(fld, '__gt__'),
            'gte': lambda fld: getattr(fld, '__ge__'),
            'lt': lambda fld: getattr(fld, '__lt__'),
            'lte': lambda fld: getattr(fld, '__le__'),
            'eq': lambda fld: getattr(fld, '__eq__'),
            'ne': lambda fld: getattr(fld, '__ne__'),
        }
        self.default_aggregation = 'sum'

    def _normalize_coerce_to_format(self, v):
        return self.format_lookup.get(v, v)

    def _normalize_coerce_to_field(self, v):
        if isinstance(v, _str_type):
            return {'field': v}
        else:
            return v

    def _normalize_coerce_to_list(self, v):
        if self._validate_type_scalar(v):
            return [v]
        else:
            return v

    def _normalize_default_setter_utcnow(self, document):
        print document
        return datetime.utcnow()

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
        elif len(results) > 1:
            self._error(field, 'Must contain only one of {}'.format(keys))


ingredient_schema = {
    'kind': {
        'type': 'string',
        'required': False,
        'default': 'Metric'
    },
    'field': {
        'schema': 'field',
        'type': 'dict',
        'coerce': 'to_field'
    },
    'format': {
        'type': 'string',
        'coerce': 'to_format'
    },
    'ts': {
        'default_setter': 'utcnow'
    }
}

field_schema = {
    'value': {
        'type': 'string',
        'required': True,
    },
    'aggregation': {
        'type': 'string',
        'required': False,
        'nullable': True,
        'default': None,
    },
    'condition': {
        'schema': 'condition',
        'contains_oneof': ['in', 'gt'],
        'required': False,
        'default': None,
        'allow_unknown': False
    }
}

condition_schema = {
    'condition': {
        'type': 'string',
        'required': False
    },
    'field': {
        'schema': 'field',
        'allow_unknown': False,
        'required': True
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
schema_registry.add('condition', condition_schema)
schema_registry.add('ingredient', ingredient_schema)

if __name__ == '__main__':
    v = IngredientValidator(ingredient_schema, allow_unknown=True)
    testers = [
        {
            'kind': 'moo',
            'format': 'comma'
        },
        {
            'kind': 'moo',
            'format': 'comma',
            'icon': 'foo',
            'field': {
                'value': 'cow',
                'condition': {
                    'field': 'moo2',
                    'in': 'wo',
                    # 'gt': 2
                }
            }
        }
    ]

    from pprint import pprint
    for d in testers:
        print('\n\nTESTING')
        pprint(d)
        if v.validate(d):
            print "We're good! Normalized is..."
            pprint(v.normalized(d))
        else:
            print 'Not good!'
            print v.errors
