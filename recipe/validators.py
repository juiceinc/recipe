"""
Validators use cerberus to validate ingredients.yaml definitions
and convert them to a normalized structure. These definitions are used
by Shelf.from_validated_yaml to construct a Shelf using a table.
"""

import logging
from collections import OrderedDict
from copy import deepcopy
from datetime import date, datetime

from cerberus import Validator, schema_registry
from cerberus.platform import _int_types, _str_type
from sqlalchemy import Float, Integer, String, case, distinct, func

from recipe.schemas import RecipeSchemas

logging.captureWarnings(True)


class IngredientValidator(Validator):
    """ IngredientValidator
    """

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
        'in': 'in_',
        'gt': '__gt__',
        'gte': '__ge__',
        'lt': '__lt__',
        'lte': '__le__',
        'eq': '__eq__',
        'ne': '__ne__',
    }

    operator_lookup = {
        '+': lambda fld: getattr(fld, '__add__'),
        '-': lambda fld: getattr(fld, '__sub__'),
        '/': lambda fld: getattr(fld, '__div__'),
        '*': lambda fld: getattr(fld, '__mul__'),
    }

    default_aggregation = 'sum'

    def __init__(self, *args, **kwargs):
        # Set defaults
        kwargs['schema'] = kwargs.get('schema', 'Ingredient')
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

        def tokenize(s):
            """ Tokenize a string by splitting it by + and -

            >>> tokenize('this + that')
            ['this', '+', 'that']

            >>> tokenize('this+that')
            ['this', '+', 'that']

            >>> tokenize('this+that-other')
            ['this', '+', 'that', '-', 'other]
            """

            # Crude tokenization
            s = s.replace('+', ' + ').replace('-', ' - ') \
                .replace('/', ' / ').replace('*', ' * ')
            words = [w for w in s.split(' ') if w]
            return words

        if isinstance(v, _str_type):
            field_parts = tokenize(v)
            field = field_parts[0]
            d = {'value': field}
            if len(field_parts) > 1:
                # if we need to add and subtract from the field
                # join the field parts into pairs, for instance if field parts is
                # [MyTable.first, '-', MyTable.second, '+', MyTable.third]
                # we will get two pairs here
                # [('-', MyTable.second), ('+', MyTable.third)]
                d['operators'] = []
                for operator, other_field in zip(
                    field_parts[1::2], field_parts[2::2]
                ):
                    d['operators'].append({
                        'operator': operator,
                        'field': {
                            'value': other_field
                        }
                    })
            return d
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
                value = document[k]
                cond = self.condition_lookup[k]
                return lambda fld: getattr(fld, cond)(value)
        return None

    def _normalize_default_setter_condition(self, document):
        for k in self.condition_lookup.keys():
            if k in document:
                value = document[k]
                cond = self.condition_lookup[k]
                return lambda fld: getattr(fld, cond)(value)
        return None

    def _normalize_default_setter_aggregation(self, document):
        aggr = document.get('aggregation', None)
        try:
            return self.aggregation_lookup.get(aggr, None)
        except TypeError:
            # aggr is something we can't lookup (like a list)
            return None

    def test_aggregation_condition(self, subdocument=None):
        """ Test that _aggregation and _condition have been added to a
        normalized document and pop them out so that the rest of the document
        can be checked against an expected value """
        if subdocument is None:
            # Start with the normalized document
            subdocument = self.document
        if isinstance(subdocument, dict):
            for k in subdocument.keys():
                if k == '_condition':
                    assert callable(subdocument.get(k, None))
                    subdocument.pop(k)
                if k == '_fields':
                    subdocument.pop(k)
                if k == '_aggregation':
                    assert callable(subdocument.get(k, None))
                    subdocument.pop(k)
                if k in ('field', 'condition', 'operators'):
                    self.test_aggregation_condition(subdocument=subdocument[k])
        if isinstance(subdocument, list):
            for itm in subdocument:
                self.test_aggregation_condition(subdocument=itm)


RecipeSchemas(
    allowed_aggregations=IngredientValidator.aggregation_lookup.keys()
).register_schemas()
