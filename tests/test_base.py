from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
    first = Column('first', String(), primary_key=True)
    last = Column('last', String())
    age = Column('age', Integer())

    __tablename__ = 'foo'
    __table_args__ = {'extend_existing': True}

