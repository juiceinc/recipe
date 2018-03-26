# -*- coding: utf-8 -*-
from cerberus import Validator
from sqlalchemy import func
from tests.test_base import MyTable

from recipe.utils import AttrDict, disaggregate, replace_whitespace_with_space
from recipe.validators import (
    IngredientValidator, aggregated_field_schema, default_field_schema
)


class TestValidateIngredient(object):

    def setup(self):
        self.validator = IngredientValidator()
        self.field_validator = Validator(
            schema=default_field_schema, allow_unknown=False
        )

    def test_good(self):
        testers = [
            {
                'kind': 'Metric',
                'field': 'moo',
                'format': 'comma'
            },
            {
                'kind': 'Metric',
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
        for d in testers:
            if not self.validator.validate(d):
                assert False
            assert self.validator.validate(d)

    def test_ingredient_kind(self):
        # Dicts to validate and the results
        good_values = [({}, {
            'kind': 'Metric'
        }), ({
            'kind': 'Ingredient'
        }, {
            'kind': 'Ingredient'
        })]

        for d, result in good_values:
            assert self.validator.validate(d)
            self.validator.normalized(d) == result

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'kind': 'asa'
            }, "{'kind': ['unallowed value asa']}"),
            ({
                'kind': 'Sque'
            }, "{'kind': ['unallowed value Sque']}"),
        ]
        for d, result in bad_values:
            assert self.validator.validate(d) == False
            assert str(self.validator.errors) == result

    def test_ingredient_format(self):
        # Dicts to validate and the results
        good_values = [
            ({
                'format': 'comma'
            }, {
                'kind': 'Metric',
                'format': ',.0f'
            }),
            ({
                'format': ',.0f'
            }, {
                'kind': 'Metric',
                'format': ',.0f'
            }),
            ({
                'format': 'cow'
            }, {
                'kind': 'Metric',
                'format': 'cow'
            }),
        ]
        IngredientValidator.format_lookup['cow'] = '.0f "moos"'
        for d, result in good_values:
            assert self.validator.validate(d)
            self.validator.normalized(d) == result

        # We can add new format_lookups
        IngredientValidator.format_lookup['cow'] = '.0f "moos"'
        good_values = [
            ({
                'format': 'cow'
            }, {
                'kind': 'Metric',
                'format': '.0f "moos"'
            }),
        ]
        for d, result in good_values:
            assert self.validator.validate(d)
            self.validator.normalized(d) == result

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'format': 2
            }, "{'format': ['must be of string type']}"),
            ({
                'format': []
            }, "{'format': ['must be of string type']}"),
            ({
                'format': ['comma']
            }, "{'format': ['must be of string type']}"),
        ]
        for d, result in bad_values:
            assert self.validator.validate(d) == False
            assert str(self.validator.errors) == result

    def test_ingredient_field(self):
        # Dicts to validate and the results
        good_values = [({
            'field': 'moo'
        }, {
            'kind': 'Metric',
            'field': {
                'value': 'moo'
            }
        })]
        IngredientValidator.format_lookup['cow'] = '.0f "moos"'
        for d, result in good_values:
            assert self.validator.validate(d)
            self.validator.normalized(d) == result

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'field': 2
            }, "{'field': ['must be of dict type']}"),
            ({
                'field': 2.1
            }, "{'field': ['must be of dict type']}"),
            # TODO: Why don't these fail validation
            # ({'field': tuple()}, "{'field': ['must be of dict type']}"),
            # ({'field': []}, "{'field': ['must be of dict type']}"),
            # ({'field': ['comma']}, "{'field': ['must be of dict type']}"),
        ]
        for d, result in bad_values:
            assert self.validator.validate(d) == False
            assert str(self.validator.errors) == result


class TestValidateField(object):

    def setup(self):
        self.validator = IngredientValidator(
            schema=default_field_schema, allow_unknown=False
        )

    def test_field_value(self):
        # Dicts to validate and the results
        good_values = [
            ({
                'value': 'foo'
            }, {
                'value': 'foo'
            }),
        ]

        for d, result in good_values:
            assert self.validator.validate(d)
            self.validator.normalized(d) == result

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'kind': 'asa'
            }, " {'kind': ['unknown field'], 'value': ['required field']}"),
        ]
        for d, result in bad_values:
            assert self.validator.validate(d) == False
            assert str(self.validator.errors) == result
