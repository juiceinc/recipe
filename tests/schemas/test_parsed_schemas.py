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
VALID_DIMENSION_FIELDS = [
    "foo",
    "foo + moo",
    "FALSE",
]

# Invalid non-aggregated fields
INVALID_DIMENSION_FIELDS = ["fo(", "sum(foo)", "fo)", "su(x + y)"]

VALID_PARTIAL_CONDITIONS = [">10", 'in ("a", "b")', "notin (1,2,3)", "=  \t b", "=b"]


def test_valid_metric_field_parsing():
    for _ in VALID_METRIC_FIELDS:
        v = {"foo": {"kind": "metric", "field": _}, "_version": "2"}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        x["foo"].pop("_config", None)
        assert x == {
            "foo": {
                "_version": "2",
                "field": _,
                "kind": "metric",
            }
        }


def test_invalid_metric_field_parsing():
    for _ in INVALID_METRIC_FIELDS:
        v = {"foo": {"kind": "metric", "field": _}, "_version": "2"}
        with pytest.raises(E.SureError):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_ensure_aggregation():
    """ Metrics where the field doesn't aggregate get wrapped in a sum """
    v = {"foo": {"kind": "metric", "field": "foo"}, "_version": "2"}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "_config": {"field": "foo", "kind": "metric"},
            "_version": "2",
            "field": "sum(foo)",
            "kind": "metric",
        }
    }


def test_valid_dimension_field_parsing():
    for _ in VALID_DIMENSION_FIELDS:
        v = {"foo": {"kind": "dimension", "field": _}, "_version": "2"}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        x["foo"].pop("_config", None)
        assert x == {"foo": {"kind": "dimension", "field": _, "_version": "2"}}


def test_invalid_dimension_field_parsing():
    for _ in INVALID_DIMENSION_FIELDS:
        v = {"foo": {"kind": "dimension", "field": _}, "_version": "2"}
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
            "foo": {"kind": "metric", "field": "sum(foo)", "format": fmt},
            "_version": "2",
        }
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x["foo"]["format"] == expected


def test_move_extra_fields():
    """ Extra _fields get moved into the extra_fields list """
    for _ in VALID_DIMENSION_FIELDS:
        v = {
            "foo": {"kind": "dimension", "field": "moo", "other_field": _},
            "_version": "2",
        }
        result = normalize_schema(shelf_schema, v, allow_unknown=False)
        result["foo"].pop("_config", None)
        assert result == {
            "foo": {
                "kind": "dimension",
                "field": "moo",
                "extra_fields": [{"field": _, "name": "other_expression"}],
                "_version": "2",
            }
        }
    # Other fields must be non aggregates
    for _ in INVALID_DIMENSION_FIELDS:
        v = {
            "foo": {"kind": "dimension", "field": "moo", "other_field": _},
            "_version": "2",
        }
        with pytest.raises(E.SureError):
            normalize_schema(shelf_schema, v, allow_unknown=False)

    # Multiple extra fields
    v = {
        "foo": {
            "kind": "dimension",
            "field": "moo",
            "latitude_field": "lat",
            "other_field": "cow + milk",
            "longitude_field": "lng",
        },
        "_version": "2",
    }
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    result["foo"].pop("_config", None)
    assert result == {
        "foo": {
            "_version": "2",
            "extra_fields": [
                {"field": "lat", "name": "latitude_expression"},
                {"field": "lng", "name": "longitude_expression"},
                {"field": "cow + milk", "name": "other_expression"},
            ],
            "field": "moo",
            "kind": "dimension",
        }
    }


def test_bucket():
    """ Test that buckets get converted into fields"""
    content = """
_version: "2"
test:
    kind: dimension
    field: moo
    buckets:
    - label: foo
      condition: ">2"
    - label: cow
      condition: "state in (1,2)"
    - label: horse
      condition: "in (3,4)"
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert (
        result["test"]["field"]
        == 'if((moo)>2,"foo",state in (1,2),"cow",(moo)in (3,4),"horse","Not found")'
    )
    assert (
        result["test"]["extra_fields"][0]["field"]
        == "if((moo)>2,0,state in (1,2),1,(moo)in (3,4),2,9999)"
    )

    content = """
_version: "2"
test:
    kind: dimension
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
        == 'if((moo)<2,"undertwo",(moo)>"2","foo",state in ("1", "2"),"cow","Not found")'
    )
    assert (
        result["test"]["extra_fields"][0]["field"]
        == 'if((moo)<2,0,(moo)>"2",1,state in ("1", "2"),2,9999)'
    )

    content = """
_version: "2"
age_buckets:
    kind: dimension
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
        == 'if((age)<2,"babies",(age)<13,"children",(age)<20,"teens","oldsters")'
    )
    assert (
        result["age_buckets"]["extra_fields"][0]["field"]
        == "if((age)<2,0,(age)<13,1,(age)<20,2,9999)"
    )


def test_quickselects():
    """Partial quickselect conditions get converted to full conditions """
    content = """
_version: "2"
test:
    kind: DIMENSION
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
            "_config": {
                "kind": "dimension",
                "field": "moo+foo",
                "quickselects": [
                    {"name": "foo", "condition": '>"2"'},
                    {"name": "cow", "condition": 'state in ("1", "2")'},
                ],
            },
            "kind": "dimension",
            "field": "moo+foo",
            "quickselects": [
                {"condition": '(moo+foo)>"2"', "name": "foo"},
                {"condition": 'state in ("1", "2")', "name": "cow"},
            ],
            "_version": "2",
        }
    }


def test_lookup():
    """Lookup must be a dict on dimesnsions"""
    content = """
_version: "2"
test:
    kind: DIMENSION
    field: moo
    lookup: 
        foo:doo
"""
    v = yaml.safe_load(content)
    with pytest.raises(E.BadType):
        normalize_schema(shelf_schema, v, allow_unknown=False)

    content = """
_version: "2"
test:
    kind: DIMENSION
    field: moo
    lookup: 
        foo: doo
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert result == {
        "test": {
            "_config": {"kind": "dimension", "field": "moo", "lookup": {"foo": "doo"}},
            "kind": "dimension",
            "field": "moo",
            "lookup": {"foo": "doo"},
            "_version": "2",
        }
    }


def test_replace_refs():
    """ Test that field references get replaced with the field"""
    content = """
_version: "2"
ttl:
    kind: metric
    field: sum(moo)
cnt:
    kind: metric
    field: count(moo)
avg:
    kind: metric
    field: '@ttl / @cnt'
city:
    kind: dimension
    field: city
city_lat:
    kind: dimension
    field: lat
city_lng:
    kind: dimension
    field: lng
city_place:
    kind: dimension
    field: '@city'
    latitude_field: '@city_lat'
    longitude_field: '@city_lng'
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)

    assert result["avg"]["field"] == "sum(moo) / count(moo)"
    assert result["city_place"] == {
        "_config": {
            "kind": "dimension",
            "field": "city",
            "latitude_field": "lat",
            "longitude_field": "lng",
        },
        "kind": "dimension",
        "field": "city",
        "extra_fields": [
            {"field": "lat", "name": "latitude_expression"},
            {"field": "lng", "name": "longitude_expression"},
        ],
        "_version": "2",
    }
