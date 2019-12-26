""" Test sureberus shelf _version="2" schemas that use lark parser for fields """

import yaml
import pytest
from sureberus import errors as E
from sureberus import normalize_schema

from recipe.schemas import shelf_schema


# Sample valid aggregated fields
from recipe.schemas.field_grammar import field_parser
from recipe.schemas.parsed_schemas import is_agex, is_no_agex, is_partial_condition

VALID_AGG_FIELDS = [
    "sum(foo)",
    "min(foo)",
    "max(foo+moo)",
    "sum(foo + moo) / sum(b)",
    "sum(fo\t+\tnext)",
]

# Sample invalid aggregate fields
INVALID_AGG_FIELDS = ["fo(", "foo", "fo)", "su(x + y)"]

# Valid non-aggregated fields
VALID_NOAGG_FIELDS = ["foo", "foo + moo", "FALSE"]

# Invalid non-aggregated fields
INVALID_NOAGG_FIELDS = ["fo(", "sum(foo)", "fo)", "su(x + y)"]

VALID_PARTIAL_CONDS = [
    ">10", 'in ("a", "b")', "notin (1,2,3)", "=  \t b", "=b"
]


def test_tree_testers():
    for f in VALID_AGG_FIELDS:
        tree = field_parser.parse(f)
        assert is_agex(tree)
        assert not is_no_agex(tree)

    for f in VALID_NOAGG_FIELDS:
        tree = field_parser.parse(f)
        assert not is_agex(tree)
        assert is_no_agex(tree)

    for f in VALID_PARTIAL_CONDS:
        tree = field_parser.parse(f)
        print(f)
        print(tree)
        assert is_partial_condition(tree)


def test_valid_metric_field_parsing():
    for _ in VALID_AGG_FIELDS:
        v = {"foo": {"kind": "Metric", "field": _}, "_version": "2"}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x == {"foo": {"kind": "Metric", "field": _, "_version": "2"}}


def test_invalid_metric_field_parsing():
    for _ in INVALID_AGG_FIELDS:
        v = {"foo": {"kind": "Metric", "field": _}, "_version": "2"}
        with pytest.raises(E.SureError) as excinfo:
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_move_extra_fields():
    for _ in VALID_NOAGG_FIELDS:
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
    for _ in INVALID_NOAGG_FIELDS:
        v = {
            "foo": {"kind": "Dimension", "field": "moo", "other_field": _},
            "_version": "2",
        }
        with pytest.raises(E.SureError) as excinfo:
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
    bucket:
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
    bucket:
    - label: foo
      condition:  '>"2"'
    - label: cow
      condition: 'state in ("1", "2")'
"""
    v = yaml.safe_load(content)
    result = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert result["test"]["field"] == 'if(moo>"2","foo",state in ("1", "2"),"cow",NULL)'
    assert (
        result["test"]["extra_fields"][0]["field"]
        == 'if(moo>"2",0,state in ("1", "2"),1,9999)'
    )


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
