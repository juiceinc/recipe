import pytest
from mock import ANY
from sqlalchemy import funcfilter
from sureberus import errors as E
from sureberus import normalize_dict, normalize_schema

from recipe.schemas import (
    aggregated_field_schema, condition_schema, metric_schema,
    non_aggregated_field_schema
)


def test_field():
    f = aggregated_field_schema
    x = normalize_schema(f, {'value': 'foo'}, allow_unknown=False)
    assert x == {'value': 'foo', '_aggregation_fn': ANY, 'aggregation': 'sum'}

    f = aggregated_field_schema
    x = normalize_schema(f, 'foo', allow_unknown=False)
    assert x == {'value': 'foo', '_aggregation_fn': ANY, 'aggregation': 'sum'}

    f = aggregated_field_schema
    x = normalize_schema(f, 'max(a)', allow_unknown=False)
    assert x == {'value': 'a', '_aggregation_fn': ANY, 'aggregation': 'max'}


def test_aggregated_field_schema():
    x = normalize_schema(aggregated_field_schema, 'foo', allow_unknown=False)
    assert x == {'value': 'foo', '_aggregation_fn': ANY, 'aggregation': 'sum'}

    x = normalize_schema(
        aggregated_field_schema, {'value': 'moo',
                                  'aggregation': 'max'},
        allow_unknown=False
    )
    assert x == {'value': 'moo', '_aggregation_fn': ANY, 'aggregation': 'max'}

    x = normalize_schema(
        aggregated_field_schema, {'value': 'moo',
                                  'aggregation': None},
        allow_unknown=False
    )
    assert x == {'value': 'moo', '_aggregation_fn': ANY, 'aggregation': None}

    with pytest.raises(E.DisallowedValue):
        normalize_schema(
            aggregated_field_schema, {'value': 'moo',
                                      'aggregation': 'squee'},
            allow_unknown=False
        )


def test_non_aggregated_field_schema():
    x = normalize_schema(
        non_aggregated_field_schema, 'foo', allow_unknown=False
    )
    assert x == {'value': 'foo', '_aggregation_fn': ANY, 'aggregation': 'none'}

    x = normalize_schema(
        non_aggregated_field_schema, {'value': 'moo',
                                      'aggregation': 'none'},
        allow_unknown=False
    )
    assert x == {'value': 'moo', '_aggregation_fn': ANY, 'aggregation': 'none'}

    x = normalize_schema(
        non_aggregated_field_schema, {'value': 'moo',
                                      'aggregation': None},
        allow_unknown=False
    )
    assert x == {'value': 'moo', '_aggregation_fn': ANY, 'aggregation': None}

    with pytest.raises(E.DisallowedValue):
        normalize_schema(
            non_aggregated_field_schema,
            {'value': 'moo',
             'aggregation': 'max'},
            allow_unknown=False
        )

    with pytest.raises(E.DisallowedValue):
        normalize_schema(
            non_aggregated_field_schema,
            {'value': 'moo',
             'aggregation': 'squee'},
            allow_unknown=False
        )


def test_metric():
    x = normalize_schema(metric_schema, {'field': 'foo'}, allow_unknown=False)
    assert x == {
        'field': {
            'value': 'foo',
            '_aggregation_fn': ANY,
            'aggregation': 'sum'
        }
    }


def test_valid_metric():
    valid_metrics = [{
        'field': 'foo',
        'icon': 'squee'
    }, {
        'field': 'foo',
        'aggregation': 'sum',
        'icon': 'squee'
    }, {
        'field': 'foo',
        'condition': {}
    }]

    for m in valid_metrics:
        normalize_schema(metric_schema, m)


def test_invalid_metric():
    invalid_metrics = [{
        'field': {
            'value': 'foo',
            'aggregation': 'squee'
        },
        'icon': 'squee'
    }, {
        'icon': 'squee'
    }]

    for m in invalid_metrics:
        with pytest.raises(Exception):
            normalize_schema(metric_schema, m)
