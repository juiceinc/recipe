""" Test sureberus schemas """

import pytest
from mock import ANY
from sqlalchemy import funcfilter
from sureberus import errors as E
from sureberus import normalize_dict, normalize_schema

from recipe.schemas import (
    aggregated_field_schema, condition_schema, ingredient_schema,
    metric_schema, non_aggregated_field_schema, shelf_schema
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


def test_field_format():
    f = aggregated_field_schema
    x = normalize_schema(
        f, {'value': 'foo',
            'format': 'comma'}, allow_unknown=False
    )
    assert x == {
        'value': 'foo',
        '_aggregation_fn': ANY,
        'aggregation': 'sum',
        'format': ',.0f'
    }


def test_field_operators():
    f = aggregated_field_schema
    x = normalize_schema(f, 'foo   + moo', allow_unknown=False)
    assert x == {
        'value': 'foo',
        'operators': [{
            'operator': 'add',
            'field': {
                'value': 'moo'
            }
        }],
        '_aggregation_fn': ANY,
        'aggregation': 'sum'
    }

    f = aggregated_field_schema
    x = normalize_schema(f, 'foo   + moo / cows', allow_unknown=False)
    assert x == {
        'value':
            'foo',
        'operators': [{
            'operator': 'add',
            'field': {
                'value': 'moo'
            }
        }, {
            'operator': 'div',
            'field': {
                'value': 'cows'
            }
        }],
        '_aggregation_fn':
            ANY,
        'aggregation':
            'sum'
    }


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

    x = normalize_schema(
        aggregated_field_schema, {'value': 'moo',
                                  'aggregation': 'none'},
        allow_unknown=False
    )
    assert x == {'value': 'moo', '_aggregation_fn': ANY, 'aggregation': 'none'}

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

    x = normalize_schema(
        metric_schema, {'field': 'max(foo)'}, allow_unknown=False
    )
    assert x == {
        'field': {
            'value': 'foo',
            '_aggregation_fn': ANY,
            'aggregation': 'max'
        }
    }

    x = normalize_schema(
        metric_schema, {'field': 'squee(foo)'}, allow_unknown=False
    )
    assert x == {
        'field': {
            'value': 'squee(foo)',
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
        'aggregation': 'none',
        'icon': 'squee'
    }, {
        'field': 'sum(foo)',
        'icon': 'squee'
    }, {
        'field': 'squee(foo)',
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


def test_condition():
    x = normalize_schema(
        condition_schema, {'field': 'foo',
                           'gt': 22}, allow_unknown=False
    )
    assert x == {
        '_op_value': 22,
        'field': {
            '_aggregation_fn': ANY,
            'aggregation': 'none',
            'value': 'foo'
        },
        '_op': '__gt__'
    }


def test_and_condition():
    x = normalize_schema(
        condition_schema, {
            'and': [{
                'field': 'foo',
                'in': [22, 44, 55]
            }, {
                'field': 'foo',
                'notin': [41]
            }]
        },
        allow_unknown=False
    )
    assert x == {
        'and': [{
            'field': {
                '_aggregation_fn': ANY,
                'aggregation': 'none',
                'value': 'foo'
            },
            '_op_value': [22, 44, 55],
            '_op': 'in_'
        }, {
            'field': {
                '_aggregation_fn': ANY,
                'aggregation': 'none',
                'value': 'foo'
            },
            '_op': 'notin',
            '_op_value': [41]
        }]
    }


def test_valid_conditions():
    conditions = [
        {
            'field': 'foo',
            'gt': 22
        },
        {
            'field': 'foo',
            'gt': 'switch'
        },
        {
            'field': 'foo',
            'gt': 12342.11
        },
        {
            'field': 'foo',
            'gt': True
        },
        {
            'field': 'foo',
            'lte': 22
        },
        {
            'field': 'foo',
            'eq': 22
        },
        {
            'field': 'foo',
            'notin': [41]
        },
        {
            'field': 'foo',
            'in': [22, 44, 55]
        },
        {
            'and': [{
                'field': 'foo',
                'in': [22, 44, 55]
            }, {
                'field': 'foo',
                'notin': [41]
            }]
        },
    ]

    for cond in conditions:
        normalize_schema(condition_schema, cond, allow_unknown=False)


def test_invalid_conditions():
    conditions = [
        {
            'field': 'foo',
            'gt': [22]
        },
        {
            'field': 'foo',
            'gt': {
                'a': 2
            }
        },
        {
            'field': 'foo',
            'lte': {
                'a': 2
            }
        },
        {
            'field': 'foo',
            'notin': 41
        },
    ]

    for cond in conditions:
        with pytest.raises(Exception):
            normalize_schema(condition_schema, cond, allow_unknown=False)


def test_ingredient():
    v = {'kind': 'Metric', 'field': 'foo'}
    x = normalize_schema(ingredient_schema, v, allow_unknown=False)
    assert x == {
        'field': {
            '_aggregation_fn': ANY,
            'aggregation': 'sum',
            'value': 'foo'
        },
        'kind': 'Metric'
    }

    v = {'kind': 'Metric', 'field': 'max(foo)'}
    x = normalize_schema(ingredient_schema, v, allow_unknown=False)
    assert x == {
        'field': {
            '_aggregation_fn': ANY,
            'aggregation': 'max',
            'value': 'foo'
        },
        'kind': 'Metric'
    }

    v = {
        'kind': 'Metric',
        'field': {
            'value': 'foo',
            'condition': {
                'field': 'moo',
                'gt': 'cow'
            }
        }
    }
    x = normalize_schema(ingredient_schema, v, allow_unknown=False)
    assert x == {
        'field': {
            'value': 'foo',
            'aggregation': 'sum',
            'condition': {
                'field': {
                    '_aggregation_fn': ANY,
                    'aggregation': 'none',
                    'value': 'moo'
                },
                '_op': '__gt__',
                '_op_value': 'cow'
            },
            '_aggregation_fn': ANY
        },
        'kind': 'Metric'
    }


def test_shelf():
    v = {'foo': {'kind': 'Metric', 'field': 'foo'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                '_aggregation_fn': ANY,
                'aggregation': 'sum',
                'value': 'foo'
            },
            'kind': 'Metric'
        }
    }
