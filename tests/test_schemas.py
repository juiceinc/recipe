""" Test sureberus schemas """
import re
from copy import deepcopy

import pytest
from sureberus import errors as E
from sureberus import normalize_schema

from recipe.schemas import (
    aggregations, find_operators, recipe_schema, shelf_schema
)


def test_field_parsing():
    v = {'foo': {'kind': 'Metric', 'field': {'value': 'foo'}}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
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
                'aggregation': 'max',
                'value': 'a'
            },
            'kind': 'Metric'
        }
    }


def test_field_as():
    v = {'foo': {'kind': 'Metric', 'field': {'value': 'foo', 'as': 'integer'}}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                '_cast_to_datatype': 'integer',
                'aggregation': 'sum',
                'value': 'foo'
            },
            'kind': 'Metric'
        }
    }

    # Bad data type to cast to
    v = {'foo': {'kind': 'Metric', 'field': {'value': 'foo', 'as': 'squee'}}}
    with pytest.raises(E.DisallowedValue):
        normalize_schema(shelf_schema, v, allow_unknown=False)


def test_field_default():
    defaults = [24, True, 11.21243, 'heythere']

    for d in defaults:
        v = {
            'foo': {
                'kind': 'Metric',
                'field': {
                    'value': 'foo',
                    'default': d
                }
            }
        }
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x == {
            'foo': {
                'field': {
                    '_coalesce_to_value': d,
                    'aggregation': 'sum',
                    'value': 'foo'
                },
                'kind': 'Metric'
            }
        }

    # Bad data type for default
    v = {'foo': {'kind': 'Metric', 'field': {'value': 'foo', 'default': {}}}}
    with pytest.raises(E.NoneMatched):
        normalize_schema(shelf_schema, v, allow_unknown=False)


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
                'aggregation': 'sum',
                'value': 'foo'
            },
            'kind': 'Metric',
            'format': ',.0f'
        }
    }


def test_field_ref():
    v = {'foo': {'kind': 'Metric', 'field': '@foo'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                'ref': 'foo',
                'aggregation': 'sum',
                'value': 'foo'
            },
            'kind': 'Metric'
        }
    }

    v = {'foo': {'kind': 'Metric', 'field': '@foo + @moo'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                'operators': [{
                    'operator': '+',
                    'field': {
                        'ref': 'moo',
                        'value': 'moo'
                    }
                }],
                'ref':
                    'foo',
                'aggregation':
                    'sum',
                'value':
                    'foo'
            },
            'kind': 'Metric'
        }
    }


def test_find_operators():

    def process_operator(op):
        """ Make the operators easier to read """
        prefix = ''
        if 'ref' in op['field']:
            prefix = '(ref)'
        elif '_use_raw_value' in op['field']:
            prefix = '(raw)'
        return op['operator'] + prefix + op['field']['value']

    examples = [
        ('a +b ', 'a', ['+b']),
        ('foo + @moo ', 'foo', ['+(ref)moo']),
        ('a+   1.0', 'a', ['+(raw)1.0']),
        ('a+1.0-2.  4', 'a', ['+(raw)1.0', '-(raw)2.4']),
        ('a+1.0-2.4', 'a', ['+(raw)1.0', '-(raw)2.4']),
        ('a+1.0-2.4/@b', 'a', ['+(raw)1.0', '-(raw)2.4', '/(ref)b']),
        # Only if the field starts with '@' will it be evaled as a ref
        ('a+1.0-2.4/2@b', 'a', ['+(raw)1.0', '-(raw)2.4', '/2@b']),
        # If the number doesn't eval to a float, treat it as a reference
        ('a+1.0.0', 'a', ['+1.0.0']),
        ('a+.01', 'a', ['+(raw).01']),
    ]

    for v, expected_fld, expected_operators in examples:
        v = re.sub(r'\s+', '', v, flags=re.UNICODE)

        fld, operators = find_operators(v)
        operators = list(map(process_operator, operators))
        assert fld == expected_fld
        assert operators == expected_operators


def test_field_operators():
    v = {'foo': {'kind': 'Metric', 'field': 'foo   + moo', 'format': 'comma'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                'value': 'foo',
                'aggregation': 'sum',
                'operators': [{
                    'operator': '+',
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
                'aggregation':
                    'sum',
                'operators': [{
                    'operator': '+',
                    'field': {
                        'value': 'moo'
                    }
                }, {
                    'operator': '/',
                    'field': {
                        'value': 'cows'
                    }
                }]
            },
            'kind': 'Metric'
        }
    }

    # numeric values are supported
    v = {'foo': {'kind': 'Metric', 'field': 'foo   + 1.02'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                'operators': [{
                    'operator': '+',
                    'field': {
                        '_use_raw_value': True,
                        'value': '1.02'
                    }
                }],
                'aggregation':
                    'sum',
                'value':
                    'foo'
            },
            'kind': 'Metric'
        }
    }

    # numeric values are supported
    v = {'foo': {'kind': 'Metric', 'field': 'foo   + 1.02 + moo  / 523.5'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'field': {
                'operators': [{
                    'operator': '+',
                    'field': {
                        '_use_raw_value': True,
                        'value': '1.02'
                    }
                }, {
                    'operator': '+',
                    'field': {
                        'value': 'moo'
                    }
                }, {
                    'operator': '/',
                    'field': {
                        '_use_raw_value': True,
                        'value': '523.5'
                    }
                }],
                'aggregation':
                    'sum',
                'value':
                    'foo'
            },
            'kind': 'Metric'
        }
    }


def test_field_divide_by():
    v = {'foo': {'kind': 'Metric', 'field': 'foo', 'divide_by': 'moo'}}
    x = normalize_schema(shelf_schema, v, allow_unknown=False)
    assert x == {
        'foo': {
            'divide_by': {
                'aggregation': 'sum',
                'value': 'moo'
            },
            'field': {
                'aggregation': 'sum',
                'value': 'foo'
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
                'operators': [{
                    'operator': '+',
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
                            'aggregation': 'none',
                            'value': 'foo'
                        },
                        'in': [22, 44, 55],
                        '_op_value': [22, 44, 55],
                        '_op': 'in_'
                    }, {
                        'field': {
                            'aggregation': 'none',
                            'value': 'foo'
                        },
                        'notin': [41],
                        '_op': 'notin',
                        '_op_value': [41]
                    }]
                },
            },
            'kind': 'Metric'
        }
    }


def test_condition_ref():
    shelf = {
        'a': {
            'kind': 'Metric',
            'field': {
                'value': 'a',
                'condition': '@foo'
            }
        },
        'foo': {
            'field': 'b'
        }
    }
    x = normalize_schema(shelf_schema, shelf, allow_unknown=False)
    assert x == {
        'a': {
            'field': {
                'aggregation': 'sum',
                'value': 'a'
            },
            'kind': 'Metric'
        },
        'foo': {
            'field': {
                'aggregation': 'sum',
                'value': 'b'
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
            'notin': {}
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
                        'aggregation': 'none',
                        'value': 'moo'
                    },
                    'gt': 'cow',
                    '_op': '__gt__',
                    '_op_value': 'cow'
                },
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
                'aggregation': 'sum',
                'value': 'foo'
            },
            'kind': 'Metric'
        }
    }


def test_valid_ingredients():
    examples = [
        ({
            'kind': 'Metric',
            'field': 'moo',
            'format': 'comma'
        }, {
            'field': {
                'aggregation': 'sum',
                'value': 'moo'
            },
            'kind': 'Metric',
            'format': ',.0f'
        }),
        #
        ({
            'kind': 'Metric',
            'field': 'moo+foo',
            'format': 'comma'
        }, {
            'field': {
                'operators': [{
                    'operator': '+',
                    'field': {
                        'value': 'foo'
                    }
                }],
                'aggregation': 'sum',
                'value': 'moo'
            },
            'kind': 'Metric',
            'format': ',.0f'
        }),
        #
        ({
            'kind': 'Metric',
            'field': 'moo+foo-coo+cow',
            'format': 'comma'
        }, {
            'field': {
                'operators': [{
                    'operator': '+',
                    'field': {
                        'value': 'foo'
                    }
                }, {
                    'operator': '-',
                    'field': {
                        'value': 'coo'
                    }
                }, {
                    'operator': '+',
                    'field': {
                        'value': 'cow'
                    }
                }],
                'aggregation':
                    'sum',
                'value':
                    'moo'
            },
            'kind': 'Metric',
            'format': ',.0f'
        }),
        #
        ({
            'kind': 'Metric',
            'format': 'comma',
            'icon': 'foo',
            'field': {
                'value': 'cow',
                'condition': {
                    'field': 'moo2',
                    'in': 'wo',
                }
            }
        }, {
            'field': {
                'condition': {
                    'field': {
                        'aggregation': 'none',
                        'value': 'moo2'
                    },
                    'in': 'wo',
                    '_op_value': ['wo'],
                    '_op': 'in_'
                },
                'aggregation': 'sum',
                'value': 'cow'
            },
            'kind': 'Metric',
            'format': ',.0f',
            'icon': 'foo'
        }),
    ]

    for ingr, expected_output in examples:
        v = {'a': deepcopy(ingr)}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert expected_output == x['a']

    # Test that a schema can be validated more than once without harm
    for ingr, expected_output in examples:
        v = {'a': deepcopy(ingr)}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        y = normalize_schema(shelf_schema, x, allow_unknown=False)
        assert expected_output == y['a']


def test_invalid_ingredients():
    examples = [
        ({
            'kind': 'asa',
            'field': 'moo'
        }, E.DisallowedValue),
        ({
            'kind': 'Sque',
            'field': 'moo'
        }, E.DisallowedValue),
    ]

    for ingr, expected_exception in examples:
        v = {'a': deepcopy(ingr)}
        with pytest.raises(expected_exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_valid_ingredients_format():
    examples = [
        ({
            'format': 'comma',
            'field': 'moo',
        }, {
            'field': {
                'aggregation': 'sum',
                'value': 'moo'
            },
            'kind': 'Metric',
            'format': ',.0f'
        }),
        ({
            'format': ',.0f',
            'field': 'moo'
        }, {
            'field': {
                'aggregation': 'sum',
                'value': 'moo'
            },
            'kind': 'Metric',
            'format': ',.0f'
        }),
        ({
            'format': 'cow',
            'field': 'moo'
        }, {
            'field': {
                'aggregation': 'sum',
                'value': 'moo'
            },
            'kind': 'Metric',
            'format': 'cow'
        }),
        ({
            'format': 'cow',
            'field': 'grass'
        }, {
            'field': {
                'aggregation': 'sum',
                'value': 'grass'
            },
            'kind': 'Metric',
            'format': 'cow'
        }),
    ]

    for ingr, expected_output in examples:
        v = {'a': deepcopy(ingr)}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert expected_output == x['a']


def test_invalid_ingredients_format():
    """ A variety of bad formats """
    examples = [
        ({
            'format': 2,
            'field': 'moo',
        }, E.BadType),
        ({
            'format': [],
            'field': 'moo',
        }, E.CoerceUnexpectedError),
        ({
            'format': ['comma'],
            'field': 'moo',
        }, E.CoerceUnexpectedError),
    ]

    for ingr, expected_exception in examples:
        v = {'a': deepcopy(ingr)}
        with pytest.raises(expected_exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_invalid_ingredients_field():
    """ A variety of bad fields. """
    examples = [({
        'field': 2
    }, E.BadType), ({
        'field': 2.1
    }, E.BadType), ({
        'field': tuple()
    }, E.BadType), ({
        'field': []
    }, E.BadType)]

    for ingr, expected_exception in examples:
        v = {'a': deepcopy(ingr)}
        with pytest.raises(expected_exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


def test_valid_ingredients_field():
    """ A variety of good fields. """
    examples = [
        ({
            'value': 'foo'
        }, {
            'aggregation': 'sum',
            'value': 'foo'
        }),
        ({
            'value': 'foo',
            'aggregation': 'sum'
        }, {
            'aggregation': 'sum',
            'value': 'foo'
        }),
    ]

    for fld, expected in examples:
        v = {'a': {'field': deepcopy(fld)}}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x['a']['field'] == expected


def test_valid_ingredients_field_aggregation():
    """ A variety of good fields. """
    examples = [
        # Aggregation gets injected
        ({
            'value': 'moo'
        }, {
            'aggregation': 'sum',
            'value': 'moo'
        }),
        # Explicit None DOES NOT GET overridden with default_aggregation
        ({
            'value': 'qoo',
            'aggregation': None
        }, {
            'aggregation': None,
            'value': 'qoo'
        }),
        ({
            'value': 'foo',
            'aggregation': 'none'
        }, {
            'aggregation': 'none',
            'value': 'foo'
        }),
        # Other aggregations are untouched
        ({
            'value': 'foo',
            'aggregation': 'sum'
        }, {
            'aggregation': 'sum',
            'value': 'foo'
        }),
        ({
            'value': 'foo',
            'aggregation': 'count'
        }, {
            'aggregation': 'count',
            'value': 'foo'
        }),
    ]

    for fld, expected in examples:
        v = {'a': {'field': deepcopy(fld)}}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x['a']['field'] == expected

    # Test ALL the aggregations
    for k in aggregations.keys():
        v = {'a': {'field': {'value': 'moo', 'aggregation': k}}}
        expected = {'aggregation': k, 'value': 'moo'}
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x['a']['field'] == expected


def test_valid_ingredients_field_condition():
    """ A variety of good field conditions. """
    examples = [
        # Aggregation gets injected
        ({
            'field': 'cow',
            'in': ['1', '2']
        }, {
            'field': {
                'aggregation': 'none',
                'value': 'cow'
            },
            'in': ['1', '2'],
            '_op_value': ['1', '2'],
            '_op': 'in_'
        }),
        ({
            'field': 'foo',
            'in': ['1', '2']
        }, {
            'field': {
                'aggregation': 'none',
                'value': 'foo'
            },
            'in': ['1', '2'],
            '_op_value': ['1', '2'],
            '_op': 'in_'
        }),
        # Scalars get turned into lists where appropriate
        ({
            'field': 'foo',
            'in': '1'
        }, {
            'field': {
                'aggregation': 'none',
                'value': 'foo'
            },
            'in': '1',
            '_op_value': ['1'],
            '_op': 'in_'
        })
    ]

    for cond, expected in examples:
        v = {
            'a': {
                'field': {
                    'value': 'moo',
                    'aggregation': 'sum',
                    'condition': deepcopy(cond)
                }
            }
        }
        x = normalize_schema(shelf_schema, v, allow_unknown=False)
        assert x['a']['field']['condition'] == expected


def test_invalid_ingredients_field_condition():
    """ A variety of bad field conditions. """
    examples = [
        (
            {
                # A condition without a predicate
                'value': 'moo',
                'aggregation': 'sum',
                'condition': {
                    'field': 'cow'
                }
            },
            E.ExpectedOneField
        ),
        (
            {
                # A condition with two operators
                'value': 'moo',
                'aggregation': 'sum',
                'condition': {
                    'field': 'cow',
                    'in': 1,
                    'gt': 2
                }
            },
            E.DisallowedField
        ),
    ]

    for ingr, expected_exception in examples:
        v = {'a': {'field': deepcopy(ingr)}}
        with pytest.raises(expected_exception):
            normalize_schema(shelf_schema, v, allow_unknown=False)


class TestValidateRecipe(object):

    def test_nothing_required(self):
        config = {}
        assert normalize_schema(recipe_schema, config) == {}

    def test_disallow_unknown(self):
        config = {'foo': 'bar'}
        with pytest.raises(E.UnknownFields):
            normalize_schema(recipe_schema, config)

    def test_ingredient_names(self):
        config = {
            'metrics': ['foo'],
            'dimensions': ['bar'],
            'filters': ['baz']
        }
        assert normalize_schema(recipe_schema, config) == config

    def test_filter_objects(self):
        """Recipes can have in-line filters, since it's common for those to be
        specific to a particular Recipe.
        """
        config = {
            'metrics': ['foo'],
            'dimensions': ['bar'],
            'filters': [{
                'field': 'xyzzy',
                'gt': 3
            }]
        }
        assert normalize_schema(recipe_schema, config) == {
            'metrics': ['foo'],
            'dimensions': ['bar'],
            'filters': [{
                'field': {
                    'aggregation': 'none',
                    'value': 'xyzzy'
                },
                'gt': 3,
                '_op': '__gt__',
                '_op_value': 3
            }]
        }

    def test_bad_filter_objects(self):
        """Recipes can have in-line filters, since it's common for those to be
        specific to a particular Recipe.
        """
        config = {
            'metrics': ['foo'],
            'dimensions': ['bar'],
            'filters': [{
                'field': 'xyzzy',
                'gt': 3,
                'lt': 1
            }]
        }

        with pytest.raises(E.NoneMatched):
            normalize_schema(recipe_schema, config) == {
                'metrics': ['foo'],
                'dimensions': ['bar'],
                'filters': [{
                    'field': 'xyzzy',
                    'gt': 3
                }]
            }
