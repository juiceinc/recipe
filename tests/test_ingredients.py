import pytest
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from recipe import *

Base = declarative_base()
engine = create_engine('sqlite://')

TABLEDEF = '''
        CREATE TABLE IF NOT EXISTS foo
        (first text,
         last text,
         age int);
'''

# create a configured "Session" class
Session = sessionmaker(bind=engine)

engine.execute(TABLEDEF)
engine.execute(
    "insert into foo values ('hi', 'there', 5), ('hi', 'fred', 10)")


class MyTable(Base):
    """
    The `icd10_preparedness` table schema
    """
    __table__ = Table('foo', Base.metadata,
                      autoload=True,
                      autoload_with=engine)
    # Primary key MUST be specified, but it isn't used.
    __mapper_args__ = {'primary_key': __table__.c.first}


class TestIngredients(object):
    def setup(self):
        # create a Session
        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last),
            'age': Metric(func.sum(MyTable.age))
        })

    def test_ingredient_init(self):
        ingr = Ingredient()
        assert len(ingr.id) == 12
        assert isinstance(ingr.columns, list)

        # Extra properties are stored in a AttrDict
        ingr = Ingredient(foo=2)
        assert ingr.meta.foo == 2
        assert ingr.meta['foo'] == 2

        with pytest.raises(BadIngredient):
            # Formatters must be list
            ingr = Ingredient(formatters='moo')

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

        ingr = Metric(MyTable.first, id='foo', formatters=[lambda x: x + 'foo'])
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
        assert unicode(filt.filters[0]) == 'foo.first = :first_1'
        filt = d.build_filter('moo', 'eq')
        assert unicode(filt.filters[0]) == 'foo.first = :first_1'
        filt = d.build_filter('moo', 'ne')
        assert unicode(filt.filters[0]) == 'foo.first != :first_1'
        filt = d.build_filter('moo', 'lt')
        assert unicode(filt.filters[0]) == 'foo.first < :first_1'
        filt = d.build_filter('moo', 'lte')
        assert unicode(filt.filters[0]) == 'foo.first <= :first_1'
        filt = d.build_filter('moo', 'gt')
        assert unicode(filt.filters[0]) == 'foo.first > :first_1'
        filt = d.build_filter('moo', 'gte')
        assert unicode(filt.filters[0]) == 'foo.first >= :first_1'

        # operator must agree with value
        with pytest.raises(ValueError):
            filt = d.build_filter(['moo'], 'eq')
        with pytest.raises(ValueError):
            filt = d.build_filter(['moo'], 'lt')

    def test_vector_filter(self):
        d = Dimension(MyTable.first)

        # Test building scalar filters
        filt = d.build_filter(['moo'])
        assert unicode(filt.filters[0]) == 'foo.first IN (:first_1)'
        filt = d.build_filter(['moo', 'foo'])
        assert unicode(filt.filters[0]) == 'foo.first IN (:first_1, :first_2)'
        filt = d.build_filter(['moo'], operator='in')
        assert unicode(filt.filters[0]) == 'foo.first IN (:first_1)'
        filt = d.build_filter(['moo'], operator='notin')
        assert unicode(filt.filters[0]) == 'foo.first NOT IN (:first_1)'
        filt = d.build_filter(['moo', 'foo'], operator='between')
        assert unicode(filt.filters[0]) == 'foo.first BETWEEN :first_1 AND :first_2'

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

        # These two filters compare equally
        assert f1 == f2
        assert not f1 is f2
        filters.add(f1)
        filters.add(f2)
        assert len(filters) == 2

        assert unicode(f1) == "[u'foo.first = :first_1']"

    def test_filter_describe(self):
        f1 = Filter(MyTable.first == 'moo', id='moo')
        assert f1.describe() == u'(Filter)moo [u\'foo.first = :first_1\']'



class TestHaving(object):
    def test_having_cmp(self):
        """ Filters are compared on their filter expression """
        havings = set()
        f1 = Having(func.sum(MyTable.age) > 2)
        f2 = Having(func.sum(MyTable.age) > 3)

        # These two filters compare equally
        assert f1 == f2
        assert not f1 is f2
        havings.add(f1)
        havings.add(f2)
        assert len(havings) == 2

        print unicode(f1)
        assert unicode(f1) == u'[u\'sum(foo.age) > :sum_1\']'

    def test_having_describe(self):
        f1 = Having(func.sum(MyTable.age) > 2, id='moo')
        print f1.describe()
        assert f1.describe() == u'(Having)moo [u\'sum(foo.age) > :sum_1\']'
