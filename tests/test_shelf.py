import pytest
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from recipe import BadRecipe
from recipe import Dimension
from recipe import Metric
from recipe import Shelf

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


class TestShelf(object):
    def setup(self):
        # create a Session
        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last),
            'age': Metric(func.sum(MyTable.age))
        })

    def test_find(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.find('first', Dimension)
        assert ingredient.id == 'first'

        # Raise if the wrong type
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('first', Metric)

        # Raise if key not present in shelf
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('foo', Dimension)

        # Raise if key is not an ingredient or string
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

        # We can choose not to raise
        ingredient = self.shelf.find('foo', Dimension, raise_if_invalid=False)
        assert ingredient == 'foo'

        ingredient = self.shelf.find(2.0, Dimension, raise_if_invalid=False)
        assert ingredient == 2.0

        ingredient = self.shelf.find('first', Metric, raise_if_invalid=False)
        assert ingredient == 'first'

    def test_get(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.first
        assert ingredient.id == 'first'

        ingredient = self.shelf.get('first', None)
        assert ingredient.id == 'first'

        ingredient = self.shelf.get('primo', None)
        assert ingredient is None


    def test_add_to_shelf(self):
        """ We can add an ingredient to a shelf """
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find('foo', Dimension)

        # We can choose not to raise
        ingredient = self.shelf.find('foo', Dimension, raise_if_invalid=False)
        assert ingredient == 'foo'

        self.shelf['foo'] = Dimension(MyTable.last)
        ingredient = self.shelf.find('last', Dimension)
        assert ingredient.id == 'last'
