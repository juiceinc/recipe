# -*- coding: utf-8 -*-
from datetime import date

import pytest
from faker import Faker
from faker.providers import BaseProvider

from recipe.utils import (
    AttrDict,
    FakerAnonymizer,
    FakerFormatter,
    replace_whitespace_with_space,
    generate_faker_seed,
    pad_values,
)

uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


class TestUtils(object):
    def test_replace_whitespace_with_space(self):
        assert replace_whitespace_with_space("fooo    moo") == "fooo moo"
        assert replace_whitespace_with_space("fooo\n\t moo") == "fooo moo"

    def test_generate_faker_seed(self):
        assert generate_faker_seed(None) == 7701040980221191251
        assert generate_faker_seed(0) == 14973660089898329583
        assert generate_faker_seed("hi") == 5329599339005471788
        assert generate_faker_seed([]) == 15515306683186839187


class TestPadValues(object):
    def test_pad_values(self):
        """A list or tuple of values are padded to a multiple of bin size """
        with pytest.raises(Exception):
            pad_values("rocket")

        v = pad_values([])
        assert v == []

        v = pad_values(tuple())
        assert v == tuple()

        v = pad_values(("hi",))
        assert (
            "hi",
            "RECIPE-DUMMY-VAL-1",
            "RECIPE-DUMMY-VAL-2",
            "RECIPE-DUMMY-VAL-3",
            "RECIPE-DUMMY-VAL-4",
            "RECIPE-DUMMY-VAL-5",
            "RECIPE-DUMMY-VAL-6",
            "RECIPE-DUMMY-VAL-7",
            "RECIPE-DUMMY-VAL-8",
            "RECIPE-DUMMY-VAL-9",
            "RECIPE-DUMMY-VAL-10",
        ) == v
        v = pad_values(
            [
                "hi",
            ]
        )
        assert [
            "hi",
            "RECIPE-DUMMY-VAL-1",
            "RECIPE-DUMMY-VAL-2",
            "RECIPE-DUMMY-VAL-3",
            "RECIPE-DUMMY-VAL-4",
            "RECIPE-DUMMY-VAL-5",
            "RECIPE-DUMMY-VAL-6",
            "RECIPE-DUMMY-VAL-7",
            "RECIPE-DUMMY-VAL-8",
            "RECIPE-DUMMY-VAL-9",
            "RECIPE-DUMMY-VAL-10",
        ] == v

        v = pad_values(list("rocket"))
        assert v == [
            "r",
            "o",
            "c",
            "k",
            "e",
            "t",
            "RECIPE-DUMMY-VAL-1",
            "RECIPE-DUMMY-VAL-2",
            "RECIPE-DUMMY-VAL-3",
            "RECIPE-DUMMY-VAL-4",
            "RECIPE-DUMMY-VAL-5",
        ]

        v = pad_values(["a"], prefix="COW", bin_size=10)
        assert v == [
            "a",
            "COW1",
            "COW2",
            "COW3",
            "COW4",
            "COW5",
            "COW6",
            "COW7",
            "COW8",
            "COW9",
        ]


class TestAttrDict(object):
    def test_attr_dict(self):
        d = AttrDict()
        assert isinstance(d, dict)
        d.foo = 2
        assert d["foo"] == 2
        d["bar"] = 3
        assert d.bar == 3


class TestFakerFormatter(object):
    def test_formatter(self):
        formatter = FakerFormatter()
        # Faker providers can be accessed by attribute if they take no
        # arguments
        assert len(formatter.format("{fake:name}", fake=Faker())) > 0

        # They can no longer be accessed as callables
        with pytest.raises(AttributeError):
            formatter.format("{fake:name()}", fake=Faker())

        # Some attributes can't be found
        with pytest.raises(AttributeError):
            formatter.format("{fake:nm}", fake=Faker())

        # Parameterized values still work
        assert len(formatter.format("{fake:numerify|text=###}", fake=Faker())) == 3


class CowProvider(BaseProvider):
    def moo(self):
        return "moo"


class TestFakerAnonymizer(object):
    def test_anonymizer_with_NO_params(self):
        a = FakerAnonymizer("{fake:random_uppercase_letter}")

        assert a("Value") == a("Value")
        assert a("boo") in uppercase

        b = FakerAnonymizer("{fake:military_apo}")
        assert b("Value") == b("Value")
        assert b("boo") == b("boo")
        assert b("Value") != b("boo")

    def test_anonymizer_with_params(self):
        a = FakerAnonymizer("{fake:numerify|text=###}")
        assert a("Value") == a("Value")

        b = FakerAnonymizer(
            "{fake:lexify|text=???,letters=abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ}"
        )
        assert len(b("value"))

        # Show we handle booleans
        before_today = FakerAnonymizer(
            "{fake:date_this_century|before_today=True,after_today=False}"
        )
        after_today = FakerAnonymizer(
            "{fake:date_this_century|before_today=False,after_today=True}"
        )

        # FakerAnonymizer always returns a string
        today = str(date.today())
        for let in "abcdefghijklmnopq":
            assert before_today(let) < today
            assert after_today(let) > today

    def test_anonymizer_with_postprocessor(self):
        # FakerAnonymizer always returns string unless converted
        a = FakerAnonymizer("{fake:ean8}")

        assert isinstance(a("Value"), str)

        b = FakerAnonymizer("{fake:ean8}", postprocessor=lambda x: int(x))

        assert isinstance(b("Value"), int)

        assert int(a("Value")) == b("Value")

    def test_anonymizer_with_provider(self):
        """Register a provider"""
        a = FakerAnonymizer("{fake:moo}", providers=[CowProvider])

        assert isinstance(a("Value"), str)
        assert a("Value") == "moo"

    def test_anonymizer_with_bad_providers(self):
        """Register a provider"""
        a = FakerAnonymizer("{fake:moo}", providers=[None, 4, CowProvider])

        assert isinstance(a("Value"), str)
        assert a("Value") == "moo"

    def test_anonymizer_with_stringprovider(self):
        """Register a string provider that is dynamically imported"""
        a = FakerAnonymizer("{fake:foo}", providers=["recipe.utils.TestProvider"])

        assert isinstance(a("Value"), str)
        assert a("Value") == "foo"

    def test_anonymizer_with_multipleproviders(self):
        """Register multiple providers"""
        a = FakerAnonymizer(
            "{fake:foo} {fake:moo}",
            providers=["recipe.utils.TestProvider", CowProvider],
        )

        assert isinstance(a("Value"), str)
        assert a("Value") == "foo moo"
