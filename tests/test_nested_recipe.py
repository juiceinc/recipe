import pytest
from sqlalchemy import Column, Float, Integer, String, Table, distinct, func
from sqlalchemy.ext.declarative import declarative_base

from recipe import (
    BadRecipe, Dimension, Having, Metric, Recipe, Shelf, get_oven
)

Base = declarative_base()
oven = get_oven('sqlite://')

TABLEDEF = '''
        CREATE TABLE IF NOT EXISTS foo
        (first text,
         last text,
         age int);
'''

oven.engine.execute(TABLEDEF)
oven.engine.execute(
    "insert into foo values ('hi', 'there', 5), ('hi', 'fred', 10)"
)


class MyTable(Base):
    first = Column('first', String(), primary_key=True)
    last = Column('last', String())
    age = Column('age', Integer())

    __tablename__ = 'foo'
    __table_args__ = {'extend_existing': True}


MyTable2 = Table('foo', Base.metadata, autoload=True, autoload_with=oven.engine)

mytable_shelf = Shelf({
    'first': Dimension(MyTable.first),
    'last': Dimension(MyTable.last),
    'age': Metric(func.sum(MyTable.age)),
})

r = Recipe(
    shelf=mytable_shelf, session=oven.Session()
).metrics('age').dimensions('first')


class Cust(Base):
    __table__ = r.as_table()


Cust2 = Table(
    'employees', meta, Column('employee_id', Integer, primary_key=True),
    Column('employee_name', String(60), nullable=False, key='name'),
    Column('employee_dept', Integer, ForeignKey('departments.department_id'))
)

nested_shelf = Shelf({
    'first': Dimension(Cust.first),
    'age': Metric(func.sum(Cust.age)),
})

r2 = Recipe(
    shelf=nested_shelf, session=oven.Session()
).dimensions('first').metrics('age')

print r2.to_sql()
print r2.all()
# Base = declarative_base()
# Base2 = declarative_base()
# Base == Base2
# Base
# Base2
# MyTable
# MyTable.__bases__
# r
# r.table
# r.table()
# r.query().selectable.froms[0]
# r.dimensions()
# d=r.dimensions()[0]
# d
# d = r.dimensions()
# d
# d = r.shelf
# d
# d = r._shelf
# d
# d[0]
# type(d)
# i =d.ingredients.values()[0]
# i =d.ingredients
# i
# i =d.ingredients()
# i
# i = d.ingredients()[0]
# i
# i.columns[0]
# c  = i.columns[0]
# c
# r._table
# a=MyTable.age
# a
# a.get_history??
# a.expression
# c = a.expression.table
# c
# history
