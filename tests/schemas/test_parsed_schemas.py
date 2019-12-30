""" Test sureberus shelf _version="2" schemas that use lark parser for fields """

import yaml
import pytest
from sureberus import errors as E
from sureberus import normalize_schema

from recipe.schemas import shelf_schema


VALID_METRIC_FIELDS = [
    "sum(foo)",
    "min(foo)",
    "max(foo+moo)",
    "sum(foo + moo) / sum(b)",
    "sum(fo\t+\tnext)",
]

# Sample invalid aggregate fields
INVALID_METRIC_FIELDS = ["fo(", "fo)", "su(x + y)"]

# Valid non-aggregated fields
VALID_DIMENSION_FIELDS = ["foo", "foo + moo", "FALSE"]

# Invalid non-aggregated fields
INVALID_DIMENSION_FIELDS = ["fo(", "sum(foo)", "fo)", "su(x + y)"]

VALID_PARTIAL_CONDITIONS = [">10", 'in ("a", "b")', "notin (1,2,3)", "=  \t b", "=b"]


def test_valid_metric_field_parsing():
    for _ in VALID_METRIC_FIELDS:
        v = {"foo": {"kind": "Metric", "field": _}, "_version": "2"}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x == {"foo": {"kind": "Metric", "field": _, "_version": "2"}}


def test_invalid_metric_field_parsing():
    for _ in INVALID_METRIC_FIELDS:
        v = {"foo": {"kind": "Metric", "field": _}, "_version": "2"}
        with pytest.raises(E.SureError):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_ensure_aggregation():
    """ Metrics where the field doesn't aggregate get wrapped in a sum """
    v = {"foo": {"kind": "Metric", "field": "foo"}, "_version": "2"}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {"foo": {"kind": "Metric", "field": "sum(foo)", "_version": "2"}}


def test_valid_dimension_field_parsing():
    for _ in VALID_DIMENSION_FIELDS:
        v = {"foo": {"kind": "Dimension", "field": _}, "_version": "2"}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x == {"foo": {"kind": "Dimension", "field": _, "_version": "2"}}


def test_invalid_dimension_field_parsing():
    for _ in INVALID_DIMENSION_FIELDS:
        v = {"foo": {"kind": "Dimension", "field": _}, "_version": "2"}
        with pytest.raises(E.SureError):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_format():
    values = [
        ("comma", ",.0f"),
        (".2s", ".2s"),
        ("dollar1", "$,.1f"),
        ("percent1", ".1%"),
    ]
    for fmt, expected in values:
        v = {
            "foo": {"kind": "Metric", "field": "sum(foo)", "format": fmt},
            "_version": "2",
        }
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x["foo"]["format"] == expected


def test_move_extra_fields():
    """ Extra _fields get moved into the extra_fields list """
    for _ in VALID_DIMENSION_FIELDS:
        v = {
            "foo": {"kind": "Dimension", "field": "moo", "other_field": _},
            "_version": "2",
        }
        result = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert result == {
            "foo": {
                "_version": "2",
                "extra_fields": [{"field": _, "name": "other_expression"}],
                "field": "moo",
                "kind": "Dimension",
            }
        }

    # Other fields must be non aggregates
    for _ in INVALID_DIMENSION_FIELDS:
        v = {
            "foo": {"kind": "Dimension", "field": "moo", "other_field": _},
            "_version": "2",
        }
        with pytest.raises(E.SureError):
            normalize_schema(shelf_schema, v, allow_unknown=False)

    # Multiple extra fields
    v = {
        "foo": {
            "kind": "Dimension",
            "field": "moo",
            "latitude_field": "lat",
            "other_field": "cow + milk",
            "longitude_field": "lng",
        },
        "_version": "2",
    }
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert result == {
        "foo": {
            "_version": "2",
            "extra_fields": [
                {"field": "lat", "name": "latitude_expression"},
                {"field": "lng", "name": "longitude_expression"},
                {"field": "cow + milk", "name": "other_expression"},
            ],
            "field": "moo",
            "kind": "Dimension",
        }
    }


def test_bucket():
    """ Test that buckets get converted into fields"""
    content = """
_version: "2"
test:
    kind: Dimension
    field: moo
    buckets:
    - label: foo
      condition: ">2"
    - label: cow
      condition: "state in (1,2)"
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert result["test"]["field"] == 'if(moo>2,"foo",state in (1,2),"cow",NULL)'
    assert (
        result["test"]["extra_fields"][0]["field"]
        == "if(moo>2,0,state in (1,2),1,9999)"
    )

    content = """
_version: "2"
test:
    kind: Dimension
    field: moo
    buckets:
    - label: undertwo
      condition: '<2'
    - label: foo
      condition: '>"2"'
    - label: cow
      condition: 'state in ("1", "2")'
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert (
        result["test"]["field"]
        == 'if(moo<2,"undertwo",moo>"2","foo",state in ("1", "2"),"cow",NULL)'
    )
    assert (
        result["test"]["extra_fields"][0]["field"]
        == 'if(moo<2,0,moo>"2",1,state in ("1", "2"),2,9999)'
    )

    content = """
_version: "2"
age_buckets:
    kind: Dimension
    field: age
    buckets:
    - label: 'babies'
      condition: '<2'
    - label: 'children'
      condition: '<13'
    - label: 'teens'
      condition: '<20'
    buckets_default_label: 'oldsters'
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert (
        result["age_buckets"]["field"]
        == 'if(age<2,"babies",age<13,"children",age<20,"teens","oldsters")'
    )
    assert (
        result["age_buckets"]["extra_fields"][0]["field"]
        == "if(age<2,0,age<13,1,age<20,2,9999)"
    )


def test_quickselects():
    """Partial quickselect conditions get converted to full conditions """
    content = """
_version: "2"
test:
    kind: Dimension
    field: moo+foo
    quickselects:
    - name: foo
      condition:  '>"2"'
    - name: cow
      condition: 'state in ("1", "2")'
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert result == {
        "test": {
            "kind": "Dimension",
            "field": "moo+foo",
            "quickselects": [
                {"condition": '(moo+foo)>"2"', "name": "foo"},
                {"condition": 'state in ("1", "2")', "name": "cow"},
            ],
            "_version": "2",
        }
    }


def test_replace_refs():
    """ Test that field references get replaced with the field"""
    content = """
_version: "2"
ttl:
    kind: Metric
    field: sum(moo)
cnt:
    kind: Metric
    field: count(moo)
avg:
    kind: Metric
    field: '@ttl / @cnt'
city:
    kind: Dimension
    field: city
city_lat:
    kind: Dimension
    field: lat
city_lng:
    kind: Dimension
    field: lng
city_place:
    kind: Dimension
    field: '@city'
    latitude_field: '@city_lat'
    longitude_field: '@city_lng'
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)

    assert result["avg"]["field"] == "sum(moo) / count(moo)"
    assert result["city_place"] == {
        "_version": "2",
        "extra_fields": [
            {"field": "lat", "name": "latitude_expression"},
            {"field": "lng", "name": "longitude_expression"},
        ],
        "field": "city",
        "kind": "Dimension",
    }
