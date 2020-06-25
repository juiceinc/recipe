""" Test sureberus schemas """
import re
from copy import deepcopy

import pytest
from sureberus import errors as E
from sureberus import normalize_schema

from recipe.schemas.config_schemas import (
    aggregations,
    find_operators,
    shelf_schema,
)
from recipe.schemas import recipe_schema


def test_field_parsing():
    """Measure and Metric are synonyms

    All kinds are normalized, so casing of kind doesn't matter
    """
    v = {"foo": {"kind": "Metric", "field": {"value": "foo"}}, "_version": "1"}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {"field": {"aggregation": "sum", "value": "foo"}, "kind": "metric"}
    }

    v = {"foo": {"kind": "METRIC", "field": "foo"}, "_version": "1"}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {"field": {"aggregation": "sum", "value": "foo"}, "kind": "metric"}
    }

    v = {"foo": {"kind": "MEASURE", "field": "max(a)"}, "_version": "1"}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {"field": {"aggregation": "max", "value": "a"}, "kind": "metric"}
    }

    v = {"foo": {"kind": "meaSURE", "field": "max(a)"}, "_version": "1"}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {"field": {"aggregation": "max", "value": "a"}, "kind": "metric"}
    }


def test_field_as():
    v = {"foo": {"kind": "metric", "field": {"value": "foo", "as": "integer"}}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {
                "_cast_to_datatype": "integer",
                "aggregation": "sum",
                "value": "foo",
            },
            "kind": "metric",
        }
    }

    # Bad data type to cast to
    v = {"foo": {"kind": "metric", "field": {"value": "foo", "as": "squee"}}}
    with pytest.raises(E.DisallowedValue):
        normalize_schema(shelf_schema, v, allow_unknown=False)


def test_dimension_extra_fields():
    """ Extra fields get added to extra_fields list"""
    value = {
        "state": {
            "field": "state",
            "kind": "Dimension",
            "latitude_field": "state_lat",
            "longitude_field": "state_lng",
        }
    }
    x = normalize_schema(shelf_schema, value)
    assert x == {
        "state": {
            "kind": "dimension",
            "field": {"value": "state", "aggregation": "none"},
            "extra_fields": [
                {
                    "field": {"value": "state_lat", "aggregation": "none"},
                    "name": "latitude_expression",
                },
                {
                    "field": {"value": "state_lng", "aggregation": "none"},
                    "name": "longitude_expression",
                },
            ],
        }
    }


def test_field_default():
    defaults = [24, True, 11.21243, "heythere"]

    for d in defaults:
        v = {"foo": {"kind": "metric", "field": {"value": "foo", "default": d}}}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x == {
            "foo": {
                "field": {
                    "_coalesce_to_value": d,
                    "aggregation": "sum",
                    "value": "foo",
                },
                "kind": "metric",
            }
        }

    # Bad data type for default
    v = {"foo": {"kind": "metric", "field": {"value": "foo", "default": {}}}}
    with pytest.raises(E.NoneMatched):
        normalize_schema(shelf_schema, v, allow_unknown=False)


def test_field_format():
    v = {"foo": {"kind": "metric", "field": {"value": "foo"}, "format": "comma"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {"aggregation": "sum", "value": "foo"},
            "kind": "metric",
            "format": ",.0f",
        }
    }


def test_field_lookup():
    v = {"foo": {"kind": "dimension", "field": {"value": "foo"}, "lookup": "potatoe"}}
    with pytest.raises(E.BadType):
        normalize_schema(shelf_schema, v, allow_unknown=False)
    v = {
        "foo": {
            "kind": "dimension",
            "field": {"value": "foo"},
            "lookup": {"eat": "potatoe"},
        }
    }
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {"aggregation": "none", "value": "foo"},
            "kind": "dimension",
            "lookup": {"eat": "potatoe",},
        }
    }


def test_field_ref():
    v = {"foo": {"kind": "metric", "field": "@foo"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {"ref": "foo", "aggregation": "sum", "value": "foo"},
            "kind": "metric",
        }
    }

    v = {"foo": {"kind": "metric", "field": "@foo + @moo"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {
                "operators": [
                    {"operator": "+", "field": {"ref": "moo", "value": "moo"}}
                ],
                "ref": "foo",
                "aggregation": "sum",
                "value": "foo",
            },
            "kind": "metric",
        }
    }


def test_field_buckets_ref():
    v = {
        "foo": {
            "kind": "Dimension",
            "field": {
                "value": "foo",
                "condition": {"field": "foo", "label": "foo < 200", "lt": 200},
            },
        },
        "moo": {"kind": "Dimension", "field": "moo", "buckets": [{"ref": "foo"}]},
    }
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {
                "aggregation": "none",
                "condition": {
                    "_op": "__lt__",
                    "_op_value": 200,
                    "field": {"aggregation": "none", "value": "foo"},
                    "label": "foo < 200",
                    "lt": 200,
                },
                "value": "foo",
            },
            "kind": "dimension",
        },
        "moo": {
            "field": {
                "aggregation": "none",
                "buckets": [
                    {
                        "_op": "__lt__",
                        "_op_value": 200,
                        "field": {"aggregation": "none", "value": "foo"},
                        "label": "foo < 200",
                        "lt": 200,
                    }
                ],
                "buckets_default_label": "Not found",
                "value": "moo",
            },
            "kind": "dimension",
        },
    }


def test_find_operators():
    def process_operator(op):
        """ Make the operators easier to read """
        prefix = ""
        if "ref" in op["field"]:
            prefix = "(ref)"
        elif "_use_raw_value" in op["field"]:
            prefix = "(raw)"
        return op["operator"] + prefix + op["field"]["value"]

    examples = [
        ("a +b ", "a", ["+b"]),
        ("foo + @moo ", "foo", ["+(ref)moo"]),
        ("a+   1.0", "a", ["+(raw)1.0"]),
        ("a+1.0-2.  4", "a", ["+(raw)1.0", "-(raw)2.4"]),
        ("a+1.0-2.4", "a", ["+(raw)1.0", "-(raw)2.4"]),
        ("a+1.0-2.4/@b", "a", ["+(raw)1.0", "-(raw)2.4", "/(ref)b"]),
        # Only if the field starts with '@' will it be evaled as a ref
        ("a+1.0-2.4/2@b", "a", ["+(raw)1.0", "-(raw)2.4", "/2@b"]),
        # If the number doesn't eval to a float, treat it as a reference
        ("a+1.0.0", "a", ["+1.0.0"]),
        ("a+.01", "a", ["+(raw).01"]),
    ]

    for v, expected_fld, expected_operators in examples:
        v = re.sub(r"\s+", "", v, flags=re.UNICODE)

        fld, operators = find_operators(v)
        operators = list(map(process_operator, operators))
        assert fld == expected_fld
        assert operators == expected_operators


def test_field_operators():
    v = {"foo": {"kind": "metric", "field": "foo   + moo", "format": "comma"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {
                "value": "foo",
                "aggregation": "sum",
                "operators": [{"operator": "+", "field": {"value": "moo"}}],
            },
            "kind": "metric",
            "format": ",.0f",
        }
    }

    v = {"foo": {"kind": "metric", "field": "foo   + moo  / cows"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {
                "value": "foo",
                "aggregation": "sum",
                "operators": [
                    {"operator": "+", "field": {"value": "moo"}},
                    {"operator": "/", "field": {"value": "cows"}},
                ],
            },
            "kind": "metric",
        }
    }

    # numeric values are supported
    v = {"foo": {"kind": "metric", "field": "foo   + 1.02"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {
                "operators": [
                    {
                        "operator": "+",
                        "field": {"_use_raw_value": True, "value": "1.02"},
                    }
                ],
                "aggregation": "sum",
                "value": "foo",
            },
            "kind": "metric",
        }
    }

    # numeric values are supported
    v = {"foo": {"kind": "metric", "field": "foo   + 1.02 + moo  / 523.5"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "field": {
                "operators": [
                    {
                        "operator": "+",
                        "field": {"_use_raw_value": True, "value": "1.02"},
                    },
                    {"operator": "+", "field": {"value": "moo"}},
                    {
                        "operator": "/",
                        "field": {"_use_raw_value": True, "value": "523.5"},
                    },
                ],
                "aggregation": "sum",
                "value": "foo",
            },
            "kind": "metric",
        }
    }


def test_field_divide_by():
    v = {"foo": {"kind": "metric", "field": "foo", "divide_by": "moo"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {
            "divide_by": {"aggregation": "sum", "value": "moo"},
            "field": {"aggregation": "sum", "value": "foo"},
            "kind": "metric",
        }
    }


def test_valid_metric():
    valid_metrics = [
        {"kind": "metric", "field": "foo", "icon": "squee"},
        {"kind": "metric", "field": "foo", "aggregation": "sum", "icon": "squee"},
        {"kind": "metric", "field": "foo", "aggregation": "none", "icon": "squee"},
        {"kind": "metric", "field": "sum(foo)", "icon": "squee"},
        {"kind": "metric", "field": "squee(foo)", "icon": "squee"},
        {"kind": "metric", "field": "foo", "condition": {}},
    ]

    for m in valid_metrics:
        v = {"a": deepcopy(m)}
        normalize_schema(shelf_schema, v, allow_unknown=False)


def test_invalid_metric():
    invalid_metrics = [
        {
            "field": {"value": "foo", "aggregation": "squee"},
            "kind": "metric",
            "icon": "squee",
        },
        {"kind": "metric", "icon": "squee"},
    ]

    for m in invalid_metrics:
        with pytest.raises(Exception):
            v = {"a": deepcopy(m)}
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_dimension():
    v = {"a": {"kind": "Dimension", "field": "foo", "icon": "squee"}}

    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "a": {
            "field": {"aggregation": "none", "value": "foo"},
            "kind": "dimension",
            "icon": "squee",
        }
    }

    v = {"a": {"kind": "Dimension", "field": "foo + moo", "icon": "squee"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "a": {
            "field": {
                "operators": [{"operator": "+", "field": {"value": "moo"}}],
                "aggregation": "none",
                "value": "foo",
            },
            "kind": "dimension",
            "icon": "squee",
        }
    }


def test_dimension_buckets():
    v = {
        "a": {
            "kind": "Dimension",
            "field": "foo",
            "icon": "squee",
            "buckets": [{"gt": 20, "label": "over20"}],
        }
    }

    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    # The dimension field gets inserted into the buckets
    assert x == {
        "a": {
            "field": {
                "aggregation": "none",
                "buckets": [
                    {
                        "_op": "__gt__",
                        "_op_value": 20,
                        "field": {"aggregation": "none", "value": "foo"},
                        "gt": 20,
                        "label": "over20",
                    }
                ],
                "buckets_default_label": "Not found",
                "value": "foo",
            },
            "icon": "squee",
            "kind": "dimension",
        }
    }


def test_and_condition():
    shelf = {
        "a": {
            "kind": "metric",
            "field": {
                "value": "a",
                "condition": {
                    "and": [
                        {"field": "foo", "in": [22, 44, 55]},
                        {"field": "foo", "notin": [41]},
                    ]
                },
            },
        }
    }
    x = normalize_schema(shelf_schema, shelf, allow_unknown=False)
    assert x == {
        "a": {
            "field": {
                "value": "a",
                "aggregation": "sum",
                "condition": {
                    "and": [
                        {
                            "field": {"aggregation": "none", "value": "foo"},
                            "in": [22, 44, 55],
                            "_op_value": [22, 44, 55],
                            "_op": "in_",
                        },
                        {
                            "field": {"aggregation": "none", "value": "foo"},
                            "notin": [41],
                            "_op": "notin",
                            "_op_value": [41],
                        },
                    ]
                },
            },
            "kind": "metric",
        }
    }


def test_condition_ref():
    shelf = {
        "a": {"kind": "metric", "field": {"value": "a", "condition": "@foo"}},
        "foo": {"field": "b"},
    }
    x = normalize_schema(shelf_schema, shelf, allow_unknown=False)
    assert x == {
        "a": {"field": {"aggregation": "sum", "value": "a"}, "kind": "metric"},
        "foo": {"field": {"aggregation": "sum", "value": "b"}, "kind": "metric"},
    }


def test_valid_conditions():
    conditions = [
        {"field": "foo", "gt": 22},
        {"field": "foo", "gt": "switch"},
        {"field": "foo", "gt": 12342.11},
        {"field": "foo", "gt": True},
        {"field": "foo", "lte": 22},
        {"field": "foo", "eq": 22},
        {"field": "foo", "like": "moo%"},
        {"field": "foo", "ilike": "%cows%"},
        {"field": "foo", "notin": [41]},
        {"field": "foo", "in": [22, 44, 55]},
        {"field": "foo", "in": [22, 44, 55, None]},
        {
            "and": [
                {"field": "foo", "in": [22, 44, 55]},
                {"field": "foo", "notin": [41]},
            ]
        },
    ]

    shelf = {"a": {"kind": "metric", "field": {"value": "a"}}}
    for cond in conditions:
        v = deepcopy(shelf)
        v["a"]["field"]["condition"] = cond
        normalize_schema(shelf_schema, v, allow_unknown=False)


def test_invalid_conditions():
    conditions = [
        {"field": "foo", "gt": [22]},
        {"field": "foo", "gt": {"a": 2}},
        {"field": "foo", "lte": {"a": 2}},
        {"field": "foo", "notin": {}},
    ]

    shelf = {"a": {"kind": "metric", "field": {"value": "a"}}}
    for cond in conditions:
        v = deepcopy(shelf)
        v["a"]["field"]["condition"] = cond
        with pytest.raises(Exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_ingredient():
    v = {"a": {"kind": "metric", "field": "foo"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "a": {"field": {"aggregation": "sum", "value": "foo"}, "kind": "metric"}
    }

    v = {"a": {"kind": "metric", "field": "max(foo)"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "a": {"field": {"aggregation": "max", "value": "foo"}, "kind": "metric"}
    }

    v = {
        "a": {
            "kind": "metric",
            "field": {"value": "foo", "condition": {"field": "moo", "gt": "cow"}},
        }
    }
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "a": {
            "field": {
                "value": "foo",
                "aggregation": "sum",
                "condition": {
                    "field": {"aggregation": "none", "value": "moo"},
                    "gt": "cow",
                    "_op": "__gt__",
                    "_op_value": "cow",
                },
            },
            "kind": "metric",
        }
    }


def test_shelf():
    v = {"foo": {"kind": "metric", "field": "foo"}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        "foo": {"field": {"aggregation": "sum", "value": "foo"}, "kind": "metric"}
    }


def test_valid_ingredients():
    examples = [
        (
            {"kind": "metric", "field": "moo", "format": "comma"},
            {
                "field": {"aggregation": "sum", "value": "moo"},
                "kind": "metric",
                "format": ",.0f",
            },
        ),
        #
        (
            {"kind": "metric", "field": "moo+foo", "format": "comma"},
            {
                "field": {
                    "operators": [{"operator": "+", "field": {"value": "foo"}}],
                    "aggregation": "sum",
                    "value": "moo",
                },
                "kind": "metric",
                "format": ",.0f",
            },
        ),
        #
        (
            {"kind": "metric", "field": "moo+foo-coo+cow", "format": "comma"},
            {
                "field": {
                    "operators": [
                        {"operator": "+", "field": {"value": "foo"}},
                        {"operator": "-", "field": {"value": "coo"}},
                        {"operator": "+", "field": {"value": "cow"}},
                    ],
                    "aggregation": "sum",
                    "value": "moo",
                },
                "kind": "metric",
                "format": ",.0f",
            },
        ),
        #
        (
            {
                "kind": "metric",
                "format": "comma",
                "icon": "foo",
                "field": {"value": "cow", "condition": {"field": "moo2", "in": "wo"}},
            },
            {
                "field": {
                    "condition": {
                        "field": {"aggregation": "none", "value": "moo2"},
                        "in": "wo",
                        "_op_value": ["wo"],
                        "_op": "in_",
                    },
                    "aggregation": "sum",
                    "value": "cow",
                },
                "kind": "metric",
                "format": ",.0f",
                "icon": "foo",
            },
        ),
    ]

    for ingr, expected_output in examples:
        v = {"a": deepcopy(ingr)}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert expected_output == x["a"]

    # Test that a schema can be validated more than once without harm
    for ingr, expected_output in examples:
        v = {"a": deepcopy(ingr)}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        y = normalize_schema(shelf_schema, x, allow_unknown=False)
        assert expected_output == y["a"]


def test_invalid_ingredients():
    examples = [
        ({"kind": "asa", "field": "moo"}, E.DisallowedValue),
        ({"kind": "Sque", "field": "moo"}, E.DisallowedValue),
    ]

    for ingr, expected_exception in examples:
        v = {"a": deepcopy(ingr)}
        with pytest.raises(expected_exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_valid_ingredients_format():
    examples = [
        (
            {"format": "comma", "field": "moo"},
            {
                "field": {"aggregation": "sum", "value": "moo"},
                "kind": "metric",
                "format": ",.0f",
            },
        ),
        (
            {"format": ",.0f", "field": "moo"},
            {
                "field": {"aggregation": "sum", "value": "moo"},
                "kind": "metric",
                "format": ",.0f",
            },
        ),
        (
            {"format": "cow", "field": "moo"},
            {
                "field": {"aggregation": "sum", "value": "moo"},
                "kind": "metric",
                "format": "cow",
            },
        ),
        (
            {"format": "cow", "field": "grass"},
            {
                "field": {"aggregation": "sum", "value": "grass"},
                "kind": "metric",
                "format": "cow",
            },
        ),
    ]

    for ingr, expected_output in examples:
        v = {"a": deepcopy(ingr)}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert expected_output == x["a"]


def test_invalid_ingredients_format():
    """ A variety of bad formats """
    examples = [
        ({"format": 2, "field": "moo"}, E.BadType),
        ({"format": [], "field": "moo"}, E.CoerceUnexpectedError),
        ({"format": ["comma"], "field": "moo"}, E.CoerceUnexpectedError),
    ]

    for ingr, expected_exception in examples:
        v = {"a": deepcopy(ingr)}
        with pytest.raises(expected_exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_invalid_ingredients_field():
    """ A variety of bad fields. """
    examples = [
        ({"field": 2}, E.BadType),
        ({"field": 2.1}, E.BadType),
        ({"field": tuple()}, E.BadType),
        ({"field": []}, E.BadType),
    ]

    for ingr, expected_exception in examples:
        v = {"a": deepcopy(ingr)}
        with pytest.raises(expected_exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_valid_ingredients_field():
    """ A variety of good fields. """
    examples = [
        ({"value": "foo"}, {"aggregation": "sum", "value": "foo"}),
        (
            {"value": "foo", "aggregation": "sum"},
            {"aggregation": "sum", "value": "foo"},
        ),
    ]

    for fld, expected in examples:
        v = {"a": {"field": deepcopy(fld)}}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x["a"]["field"] == expected


def test_valid_ingredients_field_aggregation():
    """ A variety of good fields. """
    examples = [
        # Aggregation gets injected
        ({"value": "moo"}, {"aggregation": "sum", "value": "moo"}),
        # Explicit None DOES NOT GET overridden with default_aggregation
        ({"value": "qoo", "aggregation": None}, {"aggregation": None, "value": "qoo"}),
        (
            {"value": "foo", "aggregation": "none"},
            {"aggregation": "none", "value": "foo"},
        ),
        # Other aggregations are untouched
        (
            {"value": "foo", "aggregation": "sum"},
            {"aggregation": "sum", "value": "foo"},
        ),
        (
            {"value": "foo", "aggregation": "count"},
            {"aggregation": "count", "value": "foo"},
        ),
    ]

    for fld, expected in examples:
        v = {"a": {"field": deepcopy(fld)}}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x["a"]["field"] == expected

    # Test ALL the aggregations
    for k in aggregations.keys():
        v = {"a": {"field": {"value": "moo", "aggregation": k}}}
        expected = {"aggregation": k, "value": "moo"}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x["a"]["field"] == expected


def test_valid_ingredients_field_condition():
    """ A variety of good field conditions. """
    examples = [
        # Aggregation gets injected
        (
            {"field": "cow", "in": ["1", "2"]},
            {
                "field": {"aggregation": "none", "value": "cow"},
                "in": ["1", "2"],
                "_op_value": ["1", "2"],
                "_op": "in_",
            },
        ),
        (
            {"field": "foo", "in": ["1", "2"]},
            {
                "field": {"aggregation": "none", "value": "foo"},
                "in": ["1", "2"],
                "_op_value": ["1", "2"],
                "_op": "in_",
            },
        ),
        # Scalars get turned into lists where appropriate
        (
            {"field": "foo", "in": "1"},
            {
                "field": {"aggregation": "none", "value": "foo"},
                "in": "1",
                "_op_value": ["1"],
                "_op": "in_",
            },
        ),
    ]

    for cond, expected in examples:
        v = {
            "a": {
                "field": {
                    "value": "moo",
                    "aggregation": "sum",
                    "condition": deepcopy(cond),
                }
            }
        }
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x["a"]["field"]["condition"] == expected


def test_invalid_ingredients_field_condition():
    """ A variety of bad field conditions. """
    examples = [
        (
            {
                # A condition without a predicate
                "value": "moo",
                "aggregation": "sum",
                "condition": {"field": "cow"},
            },
            E.ExpectedOneField,
        ),
        (
            {
                # A condition with two operators
                "value": "moo",
                "aggregation": "sum",
                "condition": {"field": "cow", "in": 1, "gt": 2},
            },
            E.DisallowedField,
        ),
    ]

    for ingr, expected_exception in examples:
        v = {"a": {"field": deepcopy(ingr)}}
        with pytest.raises(expected_exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


class TestValidateRecipe(object):
    def test_nothing_required(self):
        config = {}
        assert normalize_schema(recipe_schema, config) == {}

    def test_disallow_unknown(self):
        config = {"foo": "bar"}
        with pytest.raises(E.UnknownFields):
            normalize_schema(recipe_schema, config)

    def test_ingredient_names(self):
        config = {"metrics": ["foo"], "dimensions": ["bar"], "filters": ["baz"]}
        assert normalize_schema(recipe_schema, config) == config

    def test_filter_objects(self):
        """Recipes can have in-line filters, since it's common for those to be
        specific to a particular Recipe.
        """
        config = {
            "metrics": ["foo"],
            "dimensions": ["bar"],
            "filters": [{"field": "xyzzy", "gt": 3}],
        }
        assert normalize_schema(recipe_schema, config) == {
            "metrics": ["foo"],
            "dimensions": ["bar"],
            "filters": [
                {
                    "field": {"aggregation": "none", "value": "xyzzy"},
                    "gt": 3,
                    "_op": "__gt__",
                    "_op_value": 3,
                }
            ],
        }

    def test_bad_filter_objects(self):
        """Recipes can have in-line filters, since it's common for those to be
        specific to a particular Recipe.
        """
        config = {
            "metrics": ["foo"],
            "dimensions": ["bar"],
            "filters": [{"field": "xyzzy", "gt": 3, "lt": 1}],
        }

        with pytest.raises(E.NoneMatched):
            normalize_schema(recipe_schema, config) == {
                "metrics": ["foo"],
                "dimensions": ["bar"],
                "filters": [{"field": "xyzzy", "gt": 3}],
            }
