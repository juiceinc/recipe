# -*- coding: utf-8 -*-
from sqlalchemy import func
from tests.test_base import MyTable

from recipe.utils import AttrDict, disaggregate, replace_whitespace_with_space


class TestValidators(object):

    def test_good(self):
        testers = [
            {
                'kind': 'moo',
                # 'field': 'moo',
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
        from recipe.validators import v
        for d in testers:
            assert v.validate(d)

    def test_ingredient(self):
        # kind is required
        from recipe.validators import IngredientValidator
        v = IngredientValidator()
        assert v.validate({})
        # assert v.normalized({'field': 'foo'}) == {
        #     'kind': 'Metric',
        #     'field': 'foo'
        # }
