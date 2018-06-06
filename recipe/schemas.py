"""
Registers recipe schemas
"""

import logging
from collections import OrderedDict
from copy import deepcopy

from cerberus import Validator, schema_registry
from cerberus.platform import _int_types, _str_type

logging.captureWarnings(True)


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
                OrderedDict({
                    'id_field': 'field',
                    'field': 'field'
                }),
            'Metric': {
                'field': 'aggregated_field'
            },
            'DivideMetric':
                OrderedDict({
                    'numerator_field': 'aggregated_field',
                    'denominator_field': 'aggregated_field'
                }),
            'WtdAvgMetric':
                OrderedDict({
                    'field': 'field',
                    'weight': 'field'
                }),
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
                # Allowed values are the keys of IngredientValidator.aggregation_lookup
                'allowed': self.allowed_aggregations,
                'nullable': True,
                'default': None,
            },
            'condition': {
                'schema': 'condition',
                'required': False,
                'contains_oneof':
                    list(self.nonscalar_conditions + self.scalar_conditions),
                'allow_unknown': False
            }
        }

        # Aggregated fields coerce null values to the default aggregation
        aggregated_field_schema = deepcopy(default_field_schema)
        aggregated_field_schema['aggregation']['required'] = True
        aggregated_field_schema['aggregation']['nullable'] = False
        aggregated_field_schema['aggregation']['coerce'
                                              ] = 'to_aggregation_with_default'

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

    def _register_condition_schema(self):
        condition_schema = {
            'field': {
                'schema': 'field',
                'type': 'dict',
                'coerce': 'to_field_dict',
                'allow_unknown': False,
                'required': True
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
                'allowed': self.ingredient_kinds.keys(),
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
            schema['_fields']['default'] = extras.keys()
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
