# -*- coding: utf-8 -*-
import pytest
from sureberus import normalize_schema
from sureberus.errors import UnknownFields

from recipe.validators import IngredientValidator
from recipe.schemas import recipe_schema


class TestValidateIngredient(object):

    def setup(self):
        self.validator = IngredientValidator(schema='Ingredient')

    def test_ingredients(self):
        """ Test full ingredients """
        testers = [
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
                '_fields': ['field'],
                'format': ',.0f'
            }),
            ({
                'kind': 'Metric',
                'field': 'moo+foo',
                'format': 'comma'
            }, {
                'field': {
                    'aggregation':
                        'sum',
                    'value':
                        'moo',
                    'operators': [{
                        'operator': '+',
                        'field': {
                            'aggregation': None,
                            'value': 'foo',
                        }
                    }]
                },
                'kind': 'Metric',
                '_fields': ['field'],
                'format': ',.0f'
            }),
            ({
                'kind': 'Metric',
                'field': 'moo+foo-coo+cow',
                'format': 'comma'
            }, {
                'field': {
                    'aggregation':
                        'sum',
                    'value':
                        'moo',
                    'operators': [
                        {
                            'operator': '+',
                            'field': {
                                'aggregation': None,
                                'value': 'foo',
                            }
                        },
                        {
                            'operator': '-',
                            'field': {
                                'aggregation': None,
                                'value': 'coo',
                            }
                        },
                        {
                            'operator': '+',
                            'field': {
                                'aggregation': None,
                                'value': 'cow',
                            }
                        },
                    ]
                },
                '_fields': ['field'],
                'kind': 'Metric',
                'format': ',.0f'
            }),
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
                            'aggregation': None,
                            'value': 'moo2'
                        },
                        'in': ['wo']
                    },
                    'value': 'cow',
                    'aggregation': 'sum'
                },
                '_fields': ['field'],
                'kind': 'Metric',
                'format': ',.0f',
                'icon': 'foo'
            }),
        ]
        for document, expected in testers:
            validator = IngredientValidator(
                schema=document.get('kind', 'Metric')
            )
            assert validator.validate(document)
            assert validator.document == expected

    def test_ingredient_kind(self):
        # Dicts to validate and the results
        good_values = [({
            'field': 'moo'
        }, {
            'kind': 'Metric',
            '_fields': ['field'],
            'field': {
                'value': 'moo',
                'aggregation': None
            }
        }), ({
            'kind': 'Ingredient',
            'field': 'moo',
        }, {
            'kind': 'Ingredient',
            '_fields': ['field'],
            'field': {
                'value': 'moo',
                'aggregation': None
            }
        })]

        for document, expected in good_values:
            assert self.validator.validate(document)
            assert self.validator.document == expected

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'kind': 'asa',
                'field': 'moo'
            }, "{'kind': ['unallowed value asa']}"),
            ({
                'kind': 'Sque',
                'field': 'moo'
            }, "{'kind': ['unallowed value Sque']}"),
        ]
        for d, result in bad_values:
            assert self.validator.validate(d) is False
            assert str(self.validator.errors) == result

    def test_ingredient_format(self):
        # Dicts to validate and the results
        good_values = [
            ({
                'format': 'comma',
                'field': 'moo',
            }, {
                'kind': 'Metric',
                'format': ',.0f',
                '_fields': ['field'],
                'field': {
                    'value': 'moo',
                    'aggregation': None
                }
            }),
            ({
                'format': ',.0f',
                'field': 'moo'
            }, {
                'kind': 'Metric',
                'format': ',.0f',
                '_fields': ['field'],
                'field': {
                    'value': 'moo',
                    'aggregation': None
                }
            }),
            ({
                'format': 'cow',
                'field': 'moo'
            }, {
                'kind': 'Metric',
                '_fields': ['field'],
                'field': {
                    'value': 'moo',
                    'aggregation': None
                },
                'format': 'cow'
            }),
            ({
                'format': 'cow',
                'field': 'grass'
            }, {
                'kind': 'Metric',
                '_fields': ['field'],
                'format': 'cow',
                'field': {
                    'value': 'grass',
                    'aggregation': None
                }
            }),
        ]
        for document, expected in good_values:
            assert self.validator.validate(document)
            assert self.validator.document == expected

        # We can add new format_lookups
        IngredientValidator.format_lookup['cow'] = '.0f "moos"'
        good_values = [
            ({
                'format': 'cow',
                'field': 'grass',
            }, {
                'kind': 'Metric',
                'field': {
                    'value': 'grass',
                    'aggregation': None
                },
                'format': '.0f "moos"'
            }),
        ]
        for document, expected in good_values:
            assert self.validator.validate(document)
            self.validator.document == expected

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'format': 2,
                'field': 'moo',
            }, "{'format': ['must be of string type']}"),
            ({
                'format': [],
                'field': 'moo',
            }, "{'format': ['must be of string type']}"),
            ({
                'format': ['comma'],
                'field': 'moo',
            }, "{'format': ['must be of string type']}"),
        ]
        for document, errors in bad_values:
            assert not self.validator.validate(document)
            assert str(self.validator.errors) == errors

    def test_ingredient_field(self):
        # Dicts to validate and the results
        good_values = [({
            'field': 'moo'
        }, {
            'kind': 'Metric',
            '_fields': ['field'],
            'field': {
                'value': 'moo',
                'aggregation': None
            }
        })]
        IngredientValidator.format_lookup['cow'] = '.0f "moos"'
        for document, expected in good_values:
            assert self.validator.validate(document)
            assert self.validator.document == expected

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'field': 2
            }, "{'field': ['must be of dict type']}"),
            ({
                'field': 2.1
            }, "{'field': ['must be of dict type']}"),
            ({
                'field': tuple()
            }, "{'field': ['must be of dict type']}"),
            ({
                'field': []
            }, "{'field': ['must be of dict type']}"),
        ]
        for document, errors in bad_values:
            assert not self.validator.validate(document)
            assert str(self.validator.errors) == errors


class TestValidateField(object):

    def setup(self):
        self.validator = IngredientValidator(
            schema='field', allow_unknown=False
        )

    def test_field_value(self):
        # Dicts to validate and the results
        good_values = [
            ({
                'value': 'foo'
            }, {
                'value': 'foo',
                'aggregation': None,
            }),
            ({
                'value': 'foo',
                'aggregation': 'sum'
            }, {
                'value': 'foo',
                'aggregation': 'sum'
            }),
        ]

        for document, expected in good_values:
            assert self.validator.validate(document)
            assert self.validator.document == expected

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'kind': 'asa'
            }, {
                'kind': ['unknown field'],
                'value': ['required field']
            }),
            ({
                'value': 'foo',
                'aggregation': 'cow'
            }, {
                'aggregation': ['unallowed value cow']
            }),
        ]
        for document, errors in bad_values:
            assert not self.validator.validate(document)
            assert self.validator.errors == errors


class TestValidateAggregatedField(object):

    def setup(self):
        self.validator = IngredientValidator(
            schema='aggregated_field', allow_unknown=False
        )

    def test_field_value(self):
        # Dicts to validate and the results
        good_values = [
            # Aggregation gets injected
            ({
                'value': 'moo'
            }, {
                'value': 'moo',
                'aggregation': 'sum'
            }),
            # None gets overridden with default_aggregation
            ({
                'value': 'qoo',
                'aggregation': None
            }, {
                'value': 'qoo',
                'aggregation': 'sum'
            }),
            ({
                'value': 'foo',
                'aggregation': 'none'
            }, {
                'value': 'foo',
                'aggregation': 'none'
            }),
            # Other aggregations are untouched
            ({
                'value': 'foo',
                'aggregation': 'sum'
            }, {
                'value': 'foo',
                'aggregation': 'sum'
            }),
            ({
                'value': 'foo',
                'aggregation': 'count'
            }, {
                'value': 'foo',
                'aggregation': 'count'
            }),
        ]

        for document, expected in good_values:
            assert self.validator.validate(document)
            assert self.validator.document == expected

        for k in IngredientValidator.aggregation_lookup.keys():
            document = {'value': 'foo', 'aggregation': k}
            expected = {
                'value':
                    'foo',
                'aggregation':
                    k if k else IngredientValidator.default_aggregation
            }
            assert self.validator.validate(document)
            # assert self.validator.document == expected

        # We can change the default aggregation
        IngredientValidator.default_aggregation = 'count'
        good_values = [
            # Aggregation gets injected
            ({
                'value': 'moo'
            }, {
                'value': 'moo',
                'aggregation': 'count'
            }),
            # None gets overridden with default_aggregation
            ({
                'value': 'qoo',
                'aggregation': None
            }, {
                'value': 'qoo',
                'aggregation': 'count'
            }),
            # Other aggregations are untouched
            ({
                'value': 'foo',
                'aggregation': 'none'
            }, {
                'value': 'foo',
                'aggregation': 'none'
            }),
            ({
                'value': 'foo',
                'aggregation': 'sum'
            }, {
                'value': 'foo',
                'aggregation': 'sum'
            }),
            ({
                'value': 'foo',
                'aggregation': 'count'
            }, {
                'value': 'foo',
                'aggregation': 'count'
            }),
        ]

        for document, expected in good_values:
            assert self.validator.validate(document)
            assert self.validator.document == expected

        # Dicts that fail to validate and the errors
        bad_values = [
            ({
                'kind': 'asa'
            }, {
                'kind': ['unknown field'],
                'value': ['required field']
            }),
            ({
                'value': 'foo',
                'aggregation': 'cow'
            }, {
                'aggregation': ['unallowed value cow']
            }),
            ({
                'value': 'foo',
                'aggregation': 2
            }, {
                'aggregation': ['must be of string type']
            }),
            ({
                'value': 'foo',
                'aggregation': ['sum']
            }, {
                'aggregation': ['must be of string type']
            }),
        ]
        for document, errors in bad_values:
            assert not self.validator.validate(document)
            assert self.validator.errors == errors

    def test_field_condition(self):
        # Dicts to validate and the results
        good_values = [
            # Aggregation gets injected
            ({
                'value': 'moo',
                'aggregation': 'sum',
                'condition': {
                    'field': 'cow',
                    'in': ['1', '2']
                }
            }, {
                'value': 'moo',
                'aggregation': 'sum',
                'condition': {
                    'field': {
                        'aggregation': None,
                        'value': 'cow'
                    },
                    'in': ['1', '2']
                }
            }),
        ]

        for document, expected in good_values:
            assert self.validator.validate(document)
            assert self.validator.document == expected

        error_message = {'condition': []}
        # Dicts that fail to validate and the errors
        bad_values = [
            (
                {
                    # A condition without a predicate
                    'value': 'moo',
                    'aggregation': 'sum',
                    'condition': {
                        'field': 'cow'
                    }
                },
                'Must contain one of '
                "('in', 'gt', 'gte', 'lt', 'lte', 'eq', 'ne')"
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
                'Must contain no more than one of '
                "('in', 'gt', 'gte', 'lt', 'lte', 'eq', 'ne')"
            ),
        ]
        for (document, error_message) in bad_values:
            assert not self.validator.validate(document)
            assert self.validator.errors['condition'] == [error_message]


class TestValidateCondition(object):

    def setup(self):
        self.validator = IngredientValidator(
            schema='condition', allow_unknown=False
        )

    def test_condition(self):
        # Dicts to validate and the results
        good_values = [
            ({
                'field': 'foo',
                'in': ['1', '2']
            }, {
                'field': {
                    'aggregation': None,
                    'value': 'foo'
                },
                'in': ['1', '2']
            }),
            # Scalars get turned into lists where appropriate
            ({
                'field': 'foo',
                'in': '1'
            }, {
                'field': {
                    'aggregation': None,
                    'value': 'foo'
                },
                'in': ['1']
            }),
        ]

        for document, expected in good_values:
            assert self.validator.validate(document)
            assert self.validator.document == expected

        # Dicts that fail to validate and the errors they make
        bad_values = [
            ({
                'field': 'foo',
                'kind': 'asa'
            }, "{'kind': ['unknown field']}"),
        ]
        for document, errors in bad_values:
            assert not self.validator.validate(document), \
                'should not validate; expecting {}'.format(errors)
            assert str(self.validator.errors) == errors


class TestValidateRecipe(object):

    def test_nothing_required(self):
        config = {}
        assert normalize_schema(recipe_schema, config) == {}

    def test_disallow_unknown(self):
        config = {'foo': 'bar'}
        with pytest.raises(UnknownFields):
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
            'metrics': ['foo'], 'dimensions': ['bar'],
            'filters': [{'field': 'xyzzy', 'gt': 3}]
        }
        assert normalize_schema(recipe_schema, config) == {
            'metrics': ['foo'], 'dimensions': ['bar'],
            'filters': [
                {
                    '_fields': ['field'],
                    'kind': 'Filter',
                    'field': {'aggregation': None, 'value': 'xyzzy'}, 'gt': 3},
            ]
        }

    @pytest.mark.xfail(
        strict=True,
        reason="The Cerberus-based validator isn't strict about fields")
    def test_filter_objects_disallow_unknown(self):
        config = {'filters': [{'ueoa': 'xyz', 'field': 'xyzzy', 'gt': 3}]}
        with pytest.raises(UnknownFields):
            normalize_schema(recipe_schema, config)
