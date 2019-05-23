""" Test sureberus schemas """

from copy import deepcopy

import pytest
from mock import ANY
from sureberus import normalize_schema

from recipe.schemas import shelf_schema


def test_field_parsing():
    v = {'foo': {'kind': 'Metric', 'field': {'value': 'foo'}}}
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

    v = {'foo': {'kind': 'Metric', 'field': 'max(a)'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                '_aggregation_fn': ANY,
                'aggregation': 'max',
                'value': 'a'
            },
            'kind': 'Metric'
        }
    }


def test_field_format():
    v = {
        'foo': {
            'kind': 'Metric',
            'field': {
                'value': 'foo',
            },
            'format': 'comma'
        }
    }
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                '_aggregation_fn': ANY,
                'aggregation': 'sum',
                'value': 'foo'
            },
            'kind': 'Metric',
            'format': ',.0f'
        }
    }


def test_field_operators():
    v = {'foo': {'kind': 'Metric', 'field': 'foo   + moo', 'format': 'comma'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)

    assert x == {
        'foo': {
            'field': {
                'value': 'foo',
                '_aggregation_fn': ANY,
                'aggregation': 'sum',
                'operators': [{
                    'operator': 'add',
                    'field': {
                        'value': 'moo'
                    }
                }]
            },
            'kind': 'Metric',
            'format': ',.0f'
        }
    }

    v = {'foo': {'kind': 'Metric', 'field': 'foo   + moo  / cows'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                'value':
                    'foo',
                '_aggregation_fn':
                    ANY,
                'aggregation':
                    'sum',
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
                }]
            },
            'kind': 'Metric'
        }
    }


def test_valid_metric():
    valid_metrics = [{
        'kind': 'Metric',
        'field': 'foo',
        'icon': 'squee'
    }, {
        'kind': 'Metric',
        'field': 'foo',
        'aggregation': 'sum',
        'icon': 'squee'
    }, {
        'kind': 'Metric',
        'field': 'foo',
        'aggregation': 'none',
        'icon': 'squee'
    }, {
        'kind': 'Metric',
        'field': 'sum(foo)',
        'icon': 'squee'
    }, {
        'kind': 'Metric',
        'field': 'squee(foo)',
        'icon': 'squee'
    }, {
        'kind': 'Metric',
        'field': 'foo',
        'condition': {}
    }]

    for m in valid_metrics:
        v = {'a': deepcopy(m)}
        normalize_schema(shelf_schema, v, allow_unknown=False)


def test_invalid_metric():
    invalid_metrics = [{
        'field': {
            'value': 'foo',
            'aggregation': 'squee'
        },
        'kind': 'Metric',
        'icon': 'squee'
    }, {
        'kind': 'Metric',
        'icon': 'squee'
    }]

    for m in invalid_metrics:
        with pytest.raises(Exception):
            v = {'a': deepcopy(m)}
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_dimension():
    v = {'a': {'kind': 'Dimension', 'field': 'foo', 'icon': 'squee'}}

    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'a': {
            'field': {
                '_aggregation_fn': ANY,
                'aggregation': 'none',
                'value': 'foo'
            },
            'kind': 'Dimension',
            'icon': 'squee'
        }
    }

    v = {'a': {'kind': 'Dimension', 'field': 'foo + moo', 'icon': 'squee'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'a': {
            'field': {
                '_aggregation_fn': ANY,
                'operators': [{
                    'operator': 'add',
                    'field': {
                        'value': 'moo'
                    }
                }],
                'aggregation': 'none',
                'value': 'foo'
            },
            'kind': 'Dimension',
            'icon': 'squee'
        }
    }


def test_and_condition():
    shelf = {
        'a': {
            'kind': 'Metric',
            'field': {
                'value': 'a',
                'condition': {
                    'and': [{
                        'field': 'foo',
                        'in': [22, 44, 55]
                    }, {
                        'field': 'foo',
                        'notin': [41]
                    }]
                }
            }
        }
    }
    x = normalize_schema(shelf_schema, shelf, allow_unknown=False)
    assert x == {
        'a': {
            'field': {
                'value': 'a',
                'aggregation': 'sum',
                'condition': {
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
                },
                '_aggregation_fn': ANY
            },
            'kind': 'Metric'
        }
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

    shelf = {'a': {'kind': 'Metric', 'field': {'value': 'a'}}}
    for cond in conditions:
        v = deepcopy(shelf)
        v['a']['field']['condition'] = cond
        normalize_schema(shelf_schema, v, allow_unknown=False)


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

    shelf = {'a': {'kind': 'Metric', 'field': {'value': 'a'}}}
    for cond in conditions:
        v = deepcopy(shelf)
        v['a']['field']['condition'] = cond
        with pytest.raises(Exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_ingredient():
    v = {'a': {'kind': 'Metric', 'field': 'foo'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'a': {
            'field': {
                '_aggregation_fn': ANY,
                'aggregation': 'sum',
                'value': 'foo'
            },
            'kind': 'Metric'
        }
    }

    v = {'a': {'kind': 'Metric', 'field': 'max(foo)'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'a': {
            'field': {
                '_aggregation_fn': ANY,
                'aggregation': 'max',
                'value': 'foo'
            },
            'kind': 'Metric'
        }
    }

    v = {
        'a': {
            'kind': 'Metric',
            'field': {
                'value': 'foo',
                'condition': {
                    'field': 'moo',
                    'gt': 'cow'
                }
            }
        }
    }
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'a': {
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
