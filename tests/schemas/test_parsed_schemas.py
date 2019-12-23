""" Test sureberus schemas """

import pytest
from sureberus import errors as E
from sureberus import normalize_schema

from recipe.schemas import shelf_schema


def test_valid_field_parsing():
    values = [
        "sum(foo)",
        "min(foo)",
        "max(foo+moo)",
        "sum(foo + moo) / sum(b)",
        "sum(fo\t+\tnext)",
    ]
    for _ in values:
        v = {"foo": {"kind": "Metric", "field": _}, "_version": "2"}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x == {"foo": {"kind": "Metric", "field": _, "_version": "2"}}


def test_invalid_field_parsing():
    values = [
        ("fo(", "No terminal defined for '('"),
        ("fo)", "Expecting:"),
        ("foo", "must contain an aggregate expression"),
    ]

    for _, msg in values:
        v = {"foo": {"kind": "Metric", "field": _}, "_version": "2"}
        with pytest.raises(E.SureError) as excinfo:
            normalize_schema(shelf_schema, v, allow_unknown=False)

        print("\n" + _)
        print(str(excinfo.value))
        assert msg in str(excinfo.value)
