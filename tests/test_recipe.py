from sqlalchemy import Table
from sqlalchemy import func

import recipe
from recipe import Dimension
from recipe import Having
from recipe import Metric
from recipe import Recipe
from recipe import Shelf
from .test_base import *


def test_main():
    assert recipe  # use your library here


class TestIngredients(object):
    def setup(self):
        # create a Session
        self.session = Session()

        self.shelf = Shelf({
            'first': Dimension(MyTable.first),
            'last': Dimension(MyTable.last),
            'age': Metric(func.sum(MyTable.age))
        })

    def recipe(self):
        return Recipe(shelf=self.shelf, session=self.session)

    def test_dimension(self):
        self.recipe = self.recipe().metrics('age').dimensions('first')
        assert self.recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""
        assert self.recipe.all()[0].first == 'hi'
        assert self.recipe.all()[0].age == 15
        assert self.recipe.stats.rows == 1

    def test_dimension2(self):
        self.recipe = self.recipe().metrics('age').dimensions('last').order_by(
            'last')
        assert self.recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.last AS last
FROM foo
GROUP BY foo.last
ORDER BY foo.last"""
        assert self.recipe.all()[0].last == 'fred'
        assert self.recipe.all()[0].age == 10
        assert self.recipe.stats.rows == 2

    def test_filter(self):
        self.recipe = self.recipe().metrics('age').dimensions(
            'last').filters(MyTable.age > 2).order_by('last')
        assert self.recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.last AS last
FROM foo
WHERE foo.age > 2
GROUP BY foo.last
ORDER BY foo.last"""
        assert self.recipe.all()[0].last == 'fred'
        assert self.recipe.all()[0].age == 10
        assert self.recipe.stats.rows == 2

    def test_having(self):
        hv = Having(func.sum(MyTable.age) < 10)
        self.recipe = self.recipe().metrics('age').dimensions(
            'last').filters(MyTable.age > 2).filters(hv).order_by('last')
        print self.recipe.to_sql()
        assert self.recipe.to_sql() == """SELECT sum(foo.age) AS age,
       foo.last AS last
FROM foo
WHERE foo.age > 2
GROUP BY foo.last
HAVING sum(foo.age) < 10
ORDER BY foo.last"""


def test_ingredients():
    engine = create_engine('sqlite://')

    TABLEDEF = '''
        CREATE TABLE IF NOT EXISTS foo
        (first text,
         last text,
         age int);
'''

    # create a configured "Session" class
    Session = sessionmaker(bind=engine)

    # create a Session
    session = Session()

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

    shelf = Shelf({
        'first': Dimension(MyTable.first),
        'last': Dimension(MyTable.last),
        'age': Metric(func.sum(MyTable.age))
    })

    r = Recipe(shelf=shelf, session=session).dimensions('first').metrics('age')

    assert r.to_sql() == """SELECT sum(foo.age) AS age,
       foo.first AS first
FROM foo
GROUP BY foo.first"""
    assert r.all()[0].first == 'hi'
    assert r.all()[0].age == 15
    assert r.stats.rows == 1
