""" Test sureberus schemas """
import re
from copy import deepcopy

import pytest
from sureberus import errors as E
from sureberus import normalize_schema
from sureberus.errors import ValidatorUnexpectedError

from recipe.schemas import (
    shelf_schema,
)


def test_field_parsing():
    v = {"foo": {"kind": "Metric", "field": "sum(foo)"}, "_version": "2"}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {'foo': {'kind': 'Metric', 'field': 'sum(foo)', '_version': '2'}}

def test_field_parsing_bad_parse():
    v = {"foo": {"kind": "Metric", "field": "fo("}, "_version": "2"}
    with pytest.raises(E.SureError) as excinfo:
        normalize_schema(shelf_schema, v, allow_unknown=False)
    assert "No terminal defined for '('" in str(excinfo.value)

def test_field_parsing_no_aggregate():
    v = {"foo": {"kind": "Metric", "field": "foo"}, "_version": "2"}
    with pytest.raises(E.SureError) as excinfo:
        normalize_schema(shelf_schema, v, allow_unknown=False)
    print(excinfo.value)
    assert "must contain an aggregate expression" in str(excinfo.value)
