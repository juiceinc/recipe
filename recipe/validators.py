"""
Validators use cerberus to validate ingredients.yaml definitions
and convert them to a normalized structure. These definitions are used
by Shelf.from_validated_yaml to construct a Shelf using a table.
"""

import logging
from datetime import date, datetime

from cerberus import Validator
from cerberus.platform import _int_types, _str_type
from sqlalchemy import distinct, func

from recipe.schemas import RecipeSchemas

logging.captureWarnings(True)


class IngredientValidator(Validator):
    """A validator for ingredients."""

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
                # if we need to add and subtract from the field join the field
                # parts into pairs, for instance if field parts is
                #     [MyTable.first, '-', MyTable.second, '+', MyTable.third]
                # we will get two pairs here
                #     [('-', MyTable.second), ('+', MyTable.third)]
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


RecipeSchemas(
    allowed_aggregations=list(IngredientValidator.aggregation_lookup.keys())
)
