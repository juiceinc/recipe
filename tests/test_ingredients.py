# -*- coding: utf-8 -*-

import pytest
from sqlalchemy import case, func, distinct

from recipe import (Dimension, Metric, Ingredient, BadIngredient, Filter,
                    Having, IdValueDimension, LookupDimension, DivideMetric,
                    SumIfMetric, CountIfMetric, )
from recipe.shelf import parse_field, ingredient_from_dict
from recipe.compat import str
from tests.test_base import mytable_shelf, MyTable


class TestIngredients(object):
    def setup(self):
        self.shelf = mytable_shelf

    def test_ingredient_init(self):
        ingr = Ingredient()
        assert len(ingr.id) == 12
        assert isinstance(ingr.columns, list)

        # Ids can be str
        ingr = Ingredient(id=u'ვეპხის')

        # Extra properties are stored in a AttrDict
        ingr = Ingredient(foo=2)
        assert ingr.meta.foo == 2
        assert ingr.meta['foo'] == 2

        with pytest.raises(BadIngredient):
            # Formatters must be list
            ingr = Ingredient(formatters='moo')

        with pytest.raises(BadIngredient):
            # Formatters must be list
            ingr = Ingredient(formatters=2)

        with pytest.raises(BadIngredient):
            # There must be the same number of column suffixes as columns
            ingr = Ingredient(columns=[MyTable.first, MyTable.last])
            ingr.make_column_suffixes()

        with pytest.raises(BadIngredient):
            # There must be the same number of column suffixes as columns
            ingr = Ingredient(column_suffixes=('foo',),
                              columns=[MyTable.first, MyTable.last])
            ingr.make_column_suffixes()

    def test_ingredient_make_column_suffixes(self):
        # make_column_suffixes
        # There must be the same number of column suffixes as columns
        ingr = Ingredient(column_suffixes=('_foo', '_moo'),
                          columns=[MyTable.first, MyTable.last])
        assert ingr.make_column_suffixes() == ('_foo', '_moo')

        ingr = Dimension(MyTable.first, formatters=[lambda x: x + 'foo'])
        assert ingr.make_column_suffixes() == ('_raw',)

    def test_cache_context(self):
        # Cache context is saved
        ingr = Ingredient(cache_context='foo')
        assert ingr.cache_context == 'foo'

    def test_ingredient_describe(self):
        # .describe()
        ingr = Ingredient(id='foo', columns=[MyTable.first, MyTable.last])
        assert ingr.describe() == '(Ingredient)foo MyTable.first MyTable.last'

        ingr = Dimension(MyTable.first, id='foo')
        assert ingr.describe() == '(Dimension)foo MyTable.first'

    def test_ingredient_cauldron_extras(self):
        ingr = Ingredient(id='foo', columns=[MyTable.first, MyTable.last])
        extras = list(ingr.cauldron_extras)
        assert len(extras) == 0

        ingr = Metric(MyTable.first, id='foo',
                      formatters=[lambda x: x + 'foo'])
        extras = list(ingr.cauldron_extras)
        assert extras[0][0] == 'foo'
        assert len(extras) == 1

    def test_ingredient_cmp(self):
        """ Ingredients are sorted by id """
        ingra = Ingredient(id='b', columns=[MyTable.first])
        ingrb = Ingredient(id='a', columns=[MyTable.last])
        assert ingrb < ingra


class TestIngredientBuildFilter(object):
    def test_scalar_fitler(self):
        d = Dimension(MyTable.first)

        # Test building scalar filters
        filt = d.build_filter('moo')
        assert str(filt.filters[0]) == 'foo.first = :first_1'
        filt = d.build_filter('moo', 'eq')
        assert str(filt.filters[0]) == 'foo.first = :first_1'
        filt = d.build_filter('moo', 'ne')
        assert str(filt.filters[0]) == 'foo.first != :first_1'
        filt = d.build_filter('moo', 'lt')
        assert str(filt.filters[0]) == 'foo.first < :first_1'
        filt = d.build_filter('moo', 'lte')
        assert str(filt.filters[0]) == 'foo.first <= :first_1'
        filt = d.build_filter('moo', 'gt')
        assert str(filt.filters[0]) == 'foo.first > :first_1'
        filt = d.build_filter('moo', 'gte')
        assert str(filt.filters[0]) == 'foo.first >= :first_1'

        # str filter values are acceptable
        filt = d.build_filter(u'Τη γλώσ')
        assert str(filt.filters[0]) == 'foo.first = :first_1'

        # operator must agree with value
        with pytest.raises(ValueError):
            filt = d.build_filter(['moo'], 'eq')
        with pytest.raises(ValueError):
            filt = d.build_filter(['moo'], 'lt')

    def test_vector_filter(self):
        d = Dimension(MyTable.first)

        # Test building scalar filters
        filt = d.build_filter(['moo'])
        assert str(filt.filters[0]) == 'foo.first IN (:first_1)'
        filt = d.build_filter(['moo', 'foo'])
        assert str(filt.filters[0]) == 'foo.first IN (:first_1, :first_2)'
        filt = d.build_filter(['moo'], operator='in')
        assert str(filt.filters[0]) == 'foo.first IN (:first_1)'
        filt = d.build_filter(['moo'], operator='notin')
        assert str(filt.filters[0]) == 'foo.first NOT IN (:first_1)'
        filt = d.build_filter(['moo', 'foo'], operator='between')
        assert str(
            filt.filters[0]) == 'foo.first BETWEEN :first_1 AND :first_2'

        with pytest.raises(ValueError):
            filt = d.build_filter('moo', 'in')
        # Between must have 2 values
        with pytest.raises(ValueError):
            filt = d.build_filter(['moo', 'foo', 'tru'], operator='between')
        with pytest.raises(ValueError):
            filt = d.build_filter(['moo'], operator='between')


class TestFilter(object):
    def test_filter_cmp(self):
        """ Filters are compared on their filter expression """
        filters = set()
        f1 = Filter(MyTable.first == 'moo')
        f2 = Filter(MyTable.first == 'foo')

        filters.add(f1)
        filters.add(f2)
        assert len(filters) == 2

        assert str(f1) in ("[u'foo.first = :first_1']",
                           "['foo.first = :first_1']")

    def test_expression(self):
        f = Filter(MyTable.first == 'foo')
        assert f.expression is not None

        f.columns = []
        assert f.expression is not None
        f.filters = []
        assert f.expression is None

    def test_filter_describe(self):
        f1 = Filter(MyTable.first == 'moo', id='moo')
        assert f1.describe() in (u'(Filter)moo [u\'foo.first = :first_1\']',
                                 '(Filter)moo [\'foo.first = :first_1\']')


class TestHaving(object):
    def test_having_cmp(self):
        """ Filters are compared on their filter expression """
        havings = set()
        f1 = Having(func.sum(MyTable.age) > 2)
        f2 = Having(func.sum(MyTable.age) > 3)

        havings.add(f1)
        havings.add(f2)
        assert len(havings) == 2

        assert str(f1) in (u'[u\'sum(foo.age) > :sum_1\']',
                           u'[\'sum(foo.age) > :sum_1\']')

    def test_expression(self):
        h = Having(func.sum(MyTable.age) > 2)
        assert h.expression is not None

        h.columns = []
        assert h.expression is not None
        h.filters = []
        assert h.expression is not None
        h.havings = []
        assert h.expression is None

    def test_having_describe(self):
        f1 = Having(func.sum(MyTable.age) > 2, id='moo')
        assert f1.describe() in (u'(Having)moo [u\'sum(foo.age) > :sum_1\']',
                                 '(Having)moo [\'sum(foo.age) > :sum_1\']')


class TestDimension(object):
    def test_init(self):
        d = Dimension(MyTable.first)
        assert len(d.columns) == 1
        assert len(d.group_by) == 1

        # Dimension with different id and value expressions
        d = Dimension(MyTable.first, id_expression=MyTable.last)
        assert len(d.columns) == 2
        assert len(d.group_by) == 2

    def test_dimension_cauldron_extras(self):
        d = Dimension(MyTable.first, id='moo')
        extras = list(d.cauldron_extras)
        assert len(extras) == 1
        # id gets injected in the response
        assert extras[0][0] == 'moo_id'

        d = Dimension(MyTable.first, id='moo',
                      formatters=[lambda x: x + 'moo'])
        extras = list(d.cauldron_extras)
        assert len(extras) == 2
        # formatted value and id gets injected in the response
        assert extras[0][0] == 'moo'
        assert extras[1][0] == 'moo_id'


class TestIdValueDimension(object):
    def test_init(self):
        # IdValueDimension should have two params
        with pytest.raises(TypeError):
            d = IdValueDimension(MyTable.first)

        d = IdValueDimension(MyTable.first, MyTable.last)
        assert len(d.columns) == 2
        assert len(d.group_by) == 2

    def test_dimension_cauldron_extras(self):
        d = IdValueDimension(MyTable.first, MyTable.last, id='moo')
        extras = list(d.cauldron_extras)
        assert len(extras) == 1
        # id gets injected in the response
        assert extras[0][0] == 'moo_id'

        d = IdValueDimension(MyTable.first, MyTable.last, id='moo',
                             formatters=[lambda x: x + 'moo'])
        extras = list(d.cauldron_extras)
        assert len(extras) == 2
        # formatted value and id gets injected in the response
        assert extras[0][0] == 'moo'
        assert extras[1][0] == 'moo_id'


class TestLookupDimension(object):
    def test_init(self):
        # IdValueDimension should have two params
        with pytest.raises(TypeError):
            d = LookupDimension(MyTable.first)

        # IdValueDimension lookup should be a dict
        with pytest.raises(BadIngredient):
            d = LookupDimension(MyTable.first, lookup='mouse')

        # Lookup dimension injects a formatter in the first position
        d = LookupDimension(MyTable.first, lookup={'hi': 'there'})
        assert len(d.columns) == 1
        assert len(d.group_by) == 1
        assert len(d.formatters) == 1

        # Existing formatters are preserved
        d = LookupDimension(MyTable.first, lookup={'hi': 'there'},
                            formatters=[lambda x: x + 'moo'])
        assert len(d.columns) == 1
        assert len(d.group_by) == 1
        assert len(d.formatters) == 2

        # The lookup formatter is injected before any existing formatters
        def fmt(value):
            return value + 'moo'

        d = LookupDimension(MyTable.first, lookup={'hi': 'there'},
                            formatters=[fmt])
        assert len(d.columns) == 1
        assert len(d.group_by) == 1
        assert len(d.formatters) == 2
        assert d.formatters[-1] is fmt


class TestMetric(object):
    def test_init(self):
        # Metric should have an expression
        with pytest.raises(TypeError):
            d = Metric()

        d = Metric(func.sum(MyTable.age))
        assert len(d.columns) == 1
        assert len(d.group_by) == 0
        assert len(d.filters) == 0

    def test_expression(self):
        d = Metric(func.sum(MyTable.age))
        assert d.expression is not None

        d.columns = []
        assert d.expression is None


class TestDivideMetric(object):
    def test_init(self):
        # DivideMetric should have a two expressions
        with pytest.raises(TypeError):
            d = DivideMetric()

        with pytest.raises(TypeError):
            d = DivideMetric(func.sum(MyTable.age))

        d = DivideMetric(func.sum(MyTable.age), func.sum(MyTable.age))
        assert len(d.columns) == 1
        assert len(d.group_by) == 0
        assert len(d.filters) == 0

        # Generate numerator / (denominator+epsilon) by default
        assert str(d.columns[0]) == 'CAST(sum(foo.age) AS FLOAT) / (' \
                                    'coalesce(' \
                                    'CAST(sum(foo.age) AS FLOAT), ' \
                                    ':coalesce_1) + :coalesce_2)'

        # Generate if denominator == 0 then 'zero' else numerator / denominator
        d = DivideMetric(func.sum(MyTable.age), func.sum(MyTable.age),
                         ifzero='zero')
        assert str(d.columns[0]) == \
            'CASE WHEN (CAST(sum(foo.age) AS FLOAT) = :param_1) THEN ' \
            ':param_2 ELSE CAST(sum(foo.age) AS FLOAT) / ' \
            'CAST(sum(foo.age) AS FLOAT) END'


class TestSumIfMetric(object):
    def test_init(self):
        # DivideMetric should have a two expressions
        with pytest.raises(TypeError):
            d = SumIfMetric()

        with pytest.raises(TypeError):
            d = SumIfMetric(func.sum(MyTable.age))

        d = SumIfMetric(MyTable.age > 5, func.sum(MyTable.age))
        assert len(d.columns) == 1
        assert len(d.group_by) == 0
        assert len(d.filters) == 0

        # Generate numerator / (denominator+epsilon) by default
        assert str(d.columns[0]) == \
            'sum(CASE WHEN (foo.age > :age_1) THEN sum(foo.age) END)'


class TestCountIfMetric(object):
    def test_init(self):
        # DivideMetric should have a two expressions
        with pytest.raises(TypeError):
            d = SumIfMetric()

        with pytest.raises(TypeError):
            d = CountIfMetric(func.sum(MyTable.age))

        d = CountIfMetric(MyTable.age > 5, MyTable.first)
        assert len(d.columns) == 1
        assert len(d.group_by) == 0
        assert len(d.filters) == 0

        # Generate numerator / (denominator+epsilon) by default
        assert str(d.columns[0]) == \
            'count(DISTINCT CASE WHEN (foo.age > :age_1) THEN foo.first END)'

        d = CountIfMetric(MyTable.age > 5, MyTable.first, distinct=False)
        assert len(d.columns) == 1
        assert len(d.group_by) == 0
        assert len(d.filters) == 0

        # Generate numerator / (denominator+epsilon) by default
        assert str(d.columns[0]) == \
            'count(CASE WHEN (foo.age > :age_1) THEN foo.first END)'


class TestIngredientFromObj(object):
    def test_ingredient_from_obj(self):
        m = ingredient_from_dict({
            'kind': 'Metric',
            'field': 'age'
        }, MyTable)
        assert isinstance(m, Metric)

        d = ingredient_from_dict({
            'kind': 'Dimension',
            'field': 'last'
        }, MyTable)
        assert isinstance(d, Dimension)

    def test_ingredient_from_obj_with_meta(self):
        m = ingredient_from_dict({
            'kind': 'Metric',
            'field': 'age',
            'format': 'comma'
        }, MyTable)
        assert isinstance(m, Metric)
        assert m.meta.format == ',.0f'

    def test_ingredient_from_obj_with_missing_format_meta(self):
        m = ingredient_from_dict({
            'kind': 'Metric',
            'field': 'age',
            'format': 'foo'
        }, MyTable)
        assert isinstance(m, Metric)
        assert m.meta.format == 'foo'


class TestParse(object):
    def test_parse_field_aggregation(self):
        data = [
            # Basic fields
            ('age', func.sum(MyTable.age)),
            ({'value': 'age'}, func.sum(MyTable.age)),

            # Aggregations
            ({'value': 'age',
              'aggregation': 'max'}, func.max(MyTable.age)),
            ({'value': 'age',
              'aggregation': 'sum'}, func.sum(MyTable.age)),
            ({'value': 'age',
              'aggregation': 'min'}, func.min(MyTable.age)),
            ({'value': 'age',
              'aggregation': 'avg'}, func.avg(MyTable.age)),
            ({'value': 'age',
              'aggregation': 'count_distinct'},
             func.count(distinct(MyTable.age))),

            # Date trunc
            ({'value': 'age',
              'aggregation': 'month'},
             func.date_trunc('month', MyTable.age)),

            # Conditions
            ({'value': 'age',
              'condition': None}, func.sum(MyTable.age)),
            ({'value': 'age',
              'condition': {
                  'field': 'last',
                  'in': ('Jones', 'Punjabi')
              }},
             func.sum(case([(MyTable.last.in_(('Jones', 'Punjabi')),
                             MyTable.age)]))),
        ]
        for input_field, expected_result in data:
            result = parse_field(input_field, MyTable)
            assert str(result) == str(expected_result)

    def test_parse_field_add_subtract(self):
        data = [
            # Basic fields
            ('first+last', func.sum(MyTable.first + MyTable.last)),
            ('first-last', func.sum(MyTable.first - MyTable.last)),
            ('first-last-first', func.sum(MyTable.first - MyTable.last -
                                          MyTable.first)),
        ]
        for input_field, expected_result in data:
            result = parse_field(input_field, MyTable)
            assert str(result) == str(expected_result)

    def test_parse_field_no_aggregations(self):
        data = [
            # Basic fields
            ('age', MyTable.age),
            ({'value': 'age'}, MyTable.age),

            # Conditions
            ({'value': 'age',
              'condition': None}, MyTable.age),
            ({'value': 'age',
              'condition': {
                  'field': 'last',
                  'in': ('Jones', 'Punjabi')
              }},
             case([(MyTable.last.in_(('Jones', 'Punjabi')),
                    MyTable.age)])),
        ]
        for input_field, expected_result in data:
            result = parse_field(input_field, table=MyTable,
                                 aggregated=False)
            assert str(result) == str(expected_result)

    def test_bad_field_string_definitions(self):
        bad_data = ['first+',
                    'first-',
                    'fir st-',
                    'fir st',
                    'first+last-',
                    'sum(first)',
                    'foo']
        for input_field in bad_data:
            with pytest.raises(BadIngredient):
                parse_field(input_field, MyTable)

    def test_bad_field_definitions(self):
        bad_data = ['abb',
                    {},
                    [],
                    ['abb'],
                    ['age'],
                    {'value': 'abb'},
                    {'value': ['age']},
                    {'condition': ['age']},
                    {'condition': 'foo'},
                    {'condition': []},
                    ]
        for input_field in bad_data:
            with pytest.raises(BadIngredient):
                parse_field(input_field, MyTable)
