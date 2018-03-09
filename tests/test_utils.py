# -*- coding: utf-8 -*-
from sqlalchemy import func
from tests.test_base import MyTable

from recipe.utils import AttrDict, disaggregate, replace_whitespace_with_space


class TestUtils(object):

    def test_replace_whitespace_with_space(self):
        assert replace_whitespace_with_space('fooo    moo') == 'fooo moo'
        assert replace_whitespace_with_space('fooo\n\t moo') == 'fooo moo'


class TestAttrDict(object):

    def test_attr_dict(self):
        d = AttrDict()
        assert isinstance(d, dict)
        d.foo = 2
        assert d['foo'] == 2
        d['bar'] = 3
        assert d.bar == 3
