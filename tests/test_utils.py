# -*- coding: utf-8 -*-
from datetime import date

import pytest
from faker import Faker

from recipe.compat import basestring
from recipe.utils import (
    AttrDict, FakerAnonymizer, FakerFormatter, replace_whitespace_with_space
)

uppercase = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'


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


class TestFakerFormatter(object):

    def test_formatter(self):
        formatter = FakerFormatter()
        # Faker providers can be accessed by attribute if they take no
        # arguments
        assert len(formatter.format('{fake:name}', fake=Faker())) > 0

        # They can no longer be accessed as callables
        with pytest.raises(AttributeError):
            formatter.format('{fake:name()}', fake=Faker())

        # Some attributes can't be found
        with pytest.raises(AttributeError):
            formatter.format('{fake:nm}', fake=Faker())

        # Parameterized values still work
        assert len(formatter.format('{fake:numerify|text=###}',
                                    fake=Faker())) == 3


class TestFakerAnonymizer(object):

    def test_anonymizer_with_NO_params(self):
        a = FakerAnonymizer('{fake:random_uppercase_letter}')

        assert a('Value') == a('Value')
        assert a('boo') in uppercase

        b = FakerAnonymizer('{fake:military_apo}')
        assert b('Value') == b('Value')
        assert b('boo') == b('boo')
        assert b('Value') != b('boo')

    def test_anonymizer_with_params(self):
        a = FakerAnonymizer('{fake:numerify|text=###}')
        assert a('Value') == a('Value')

        b = FakerAnonymizer(
            '{fake:lexify|text=???,letters=abcdefghijklmnopqrstuvwxyz'
            'ABCDEFGHIJKLMNOPQRSTUVWXYZ}'
        )
        assert len(b('value'))

        # Show we handle booleans
        before_today = FakerAnonymizer(
            '{fake:date_this_century|before_today=True,after_today=False}'
        )
        after_today = FakerAnonymizer(
            '{fake:date_this_century|before_today=False,after_today=True}'
        )

        # FakerAnonymizer always returns a string
        today = str(date.today())
        for let in 'abcdefghijklmnopq':
            assert before_today(let) < today
            assert after_today(let) > today

    def test_anonymizer_with_postprocessor(self):
        # FakerAnonymizer always returns string unless converted
        a = FakerAnonymizer('{fake:ean8}')

        assert isinstance(a('Value'), basestring)

        b = FakerAnonymizer('{fake:ean8}', postprocessor=lambda x: int(x))

        assert isinstance(b('Value'), int)

        assert int(a('Value')) == b('Value')
