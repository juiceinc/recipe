from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    distinct,
    func,
    Table,
)
from sqlalchemy.ext.declarative import declarative_base
from sureberus.schema import Boolean

from recipe import IdValueDimension, Dimension, Metric, Filter, Shelf, get_oven
from datetime import date
from dateutil.relativedelta import relativedelta

oven = get_oven("sqlite://")
Base = declarative_base(bind=oven.engine)


oven.engine.execute(
    """
    CREATE TABLE IF NOT EXISTS weird_table_with_column_named_true
        ("true" text);
    """
)

TABLEDEF = """
        CREATE TABLE IF NOT EXISTS foo
        (first text,
         last text,
         age int,
         birth_date date,
         dt datetime);
"""

oven.engine.execute(TABLEDEF)
oven.engine.execute(
    "insert into foo values ('hi', 'there', 5, '2005-01-01', '2005-01-01 12:15:00'), ('hi', 'fred', 10, '2015-05-15', '2013-10-15 05:20:10')"
)


# Test dynamic date filtering
TABLEDEF = """
        CREATE TABLE IF NOT EXISTS datetester
        (dt date,
         count integer);
"""

oven.engine.execute(TABLEDEF)
start_dt = date(date.today().year, date.today().month, 1)
# Add dates for the 50 months around the current date
for offset_month in range(-50, 50):
    dt = start_dt + relativedelta(months=offset_month)
    oven.engine.execute("insert into datetester values ('{}', 1)".format(dt))


# Create a table for testing summarization
TABLEDEF = """
        CREATE TABLE IF NOT EXISTS scores
        (username text,
         department text,
         testid text,
         score float,
         test_date date);
"""

oven.engine.execute(TABLEDEF)
oven.engine.execute(
    """insert into scores values
('chris', 'sales', '1', 80, '2005-01-02'),
('chip', 'ops', '2', 80, '2005-01-03'),
('chip', 'ops', '3', 90, '2005-01-04'),
('chip', 'ops', '4', 100, '2005-02-05'),
('annika', 'ops', '5', 80, '2005-02-06'),
('annika', 'ops', '6', 90, '2005-02-07')
"""
)


# Create a table for testing all data types
TABLEDEF = """
        CREATE TABLE IF NOT EXISTS datatypes
        (username text,
         department text,
         testid text,
         score float,
         test_date date,
         test_datetime datetime,
         valid_score bool
         );
"""

oven.engine.execute(TABLEDEF)
oven.engine.execute(
    """insert into datatypes values
('chris', 'sales', '1', 80, '2005-01-02', '2005-01-02 12:15:00', 0),
('chip', 'ops', '2', 80, '2005-01-03', '2005-01-03 12:20:00', 1),
('chip', 'ops', '3', 90, '2005-01-04', '2005-01-04 1:00:05', 0),
('chip', 'ops', '4', 100, '2005-02-05', '2005-02-05 16:20:33', 1),
('annika', 'ops', '5', 80, '2005-02-06', '2005-02-06 12:20:00', 0),
('annika', 'ops', '6', 90, '2005-02-07', '2005-02-07 6:44:12', 1)
"""
)


# Create a table for testing missing values
TABLEDEF = """
        CREATE TABLE IF NOT EXISTS scores_with_nulls
        (username text,
         department text,
         testid text,
         score float,
         test_date date);
"""
oven.engine.execute(TABLEDEF)
oven.engine.execute(
    """insert into scores_with_nulls values
('chris', 'sales', '1', NULL, '2005-01-02'),
('chip', 'ops', '2', 80, '2005-01-04'),
('chip', 'ops', '3', NULL, '2005-01-05'),
('chip', 'ops', '4', 100, '2005-01-07'),
('annika', NULL, '5', 80, '2005-02-01'),
('annika', NULL, '6', NULL, '2005-02-02')
"""
)


# Create a table for denormalized tables with tags
TABLEDEF = """
        CREATE TABLE IF NOT EXISTS tagscores
        (username text,
         tag text,
         department text,
         testid text,
         score float);
"""

oven.engine.execute(TABLEDEF)
oven.engine.execute(
    """insert into tagscores values
('chris', 'individual', 'sales', '1', 80),
('chris', 'manager', 'sales', '1', 80),
('chip', 'individual', 'ops', '2', 80),
('chip', 'individual', 'ops', '3', 90),
('chip', 'individual', 'ops', '4', 100),
('chip', 'musician', 'ops', '2', 80),
('chip', 'musician', 'ops', '3', 90),
('chip', 'musician', 'ops', '4', 100),
('annika', 'individual', 'ops', '5', 80),
('annika', 'individual', 'ops', '6', 90)
"""
)

oven.engine.execute(
    """CREATE TABLE IF NOT EXISTS census
(state text, sex text, age integer, pop2000 integer, pop2008 integer);"""
)
oven.engine.execute(
    """INSERT INTO CENSUS values
('Tennessee','M',0,38916,43537), ('Tennessee','M',1,38569,43343),
('Tennessee','M',2,38157,42592), ('Tennessee','M',3,37780,41530),
('Tennessee','M',4,38789,41627), ('Tennessee','M',5,39442,40758),
('Tennessee','M',6,39262,40963), ('Tennessee','M',7,40356,42287),
('Tennessee','M',8,41016,41043), ('Tennessee','M',9,42320,40944),
('Tennessee','M',10,42550,40654), ('Tennessee','M',11,41206,40248),
('Tennessee','M',12,40226,41261), ('Tennessee','M',13,39531,41969),
('Tennessee','M',14,40064,41759), ('Tennessee','M',15,40192,42830),
('Tennessee','M',16,39892,43238), ('Tennessee','M',17,40539,44452),
('Tennessee','M',18,40468,41883), ('Tennessee','M',19,41056,38818),
('Tennessee','M',20,40512,37573), ('Tennessee','M',21,38980,37588),
('Tennessee','M',22,37950,39120), ('Tennessee','M',23,37456,40392),
('Tennessee','M',24,36332,40083), ('Tennessee','M',25,38133,40499),
('Tennessee','M',26,37968,41475), ('Tennessee','M',27,38758,45410),
('Tennessee','M',28,40577,46376), ('Tennessee','M',29,43017,44094),
('Tennessee','M',30,43615,42211), ('Tennessee','M',31,40420,39868),
('Tennessee','M',32,39261,38181), ('Tennessee','M',33,39356,40291),
('Tennessee','M',34,40748,39200), ('Tennessee','M',35,43882,40644),
('Tennessee','M',36,44798,42462), ('Tennessee','M',37,43835,44641),
('Tennessee','M',38,43731,45331), ('Tennessee','M',39,43970,41921),
('Tennessee','M',40,45343,41233), ('Tennessee','M',41,43906,40829),
('Tennessee','M',42,43924,42818), ('Tennessee','M',43,43962,45398),
('Tennessee','M',44,42415,46350), ('Tennessee','M',45,43100,45330),
('Tennessee','M',46,42058,45200), ('Tennessee','M',47,39777,45227),
('Tennessee','M',48,39077,45922), ('Tennessee','M',49,38435,44568),
('Tennessee','M',50,38895,44284), ('Tennessee','M',51,38068,44243),
('Tennessee','M',52,38330,42537), ('Tennessee','M',53,40500,42786),
('Tennessee','M',54,29582,41972), ('Tennessee','M',55,30139,39294),
('Tennessee','M',56,29736,38531), ('Tennessee','M',57,30384,38026),
('Tennessee','M',58,27592,38204), ('Tennessee','M',59,25472,37067),
('Tennessee','M',60,24629,37255), ('Tennessee','M',61,23589,39018),
('Tennessee','M',62,23127,28319), ('Tennessee','M',63,21154,28739),
('Tennessee','M',64,21528,28386), ('Tennessee','M',65,20582,28521),
('Tennessee','M',66,18864,25328), ('Tennessee','M',67,18481,23030),
('Tennessee','M',68,18190,21827), ('Tennessee','M',69,17103,20597),
('Tennessee','M',70,17497,19838), ('Tennessee','M',71,15902,17716),
('Tennessee','M',72,15499,17521), ('Tennessee','M',73,14706,16467),
('Tennessee','M',74,13717,14753), ('Tennessee','M',75,13470,14055),
('Tennessee','M',76,12317,13680), ('Tennessee','M',77,11168,12304),
('Tennessee','M',78,10672,12340), ('Tennessee','M',79,9583,10930),
('Tennessee','M',80,8598,10116), ('Tennessee','M',81,7322,9449),
('Tennessee','M',82,6452,8089), ('Tennessee','M',83,5562,7854),
('Tennessee','M',84,5033,6731), ('Tennessee','M',85,22207,32331),
('Tennessee','F',0,36986,41654), ('Tennessee','F',1,36418,41207),
('Tennessee','F',2,36341,41096), ('Tennessee','F',3,36124,39945),
('Tennessee','F',4,36724,39803), ('Tennessee','F',5,37237,39650),
('Tennessee','F',6,37301,39830), ('Tennessee','F',7,37972,40617),
('Tennessee','F',8,39291,39550), ('Tennessee','F',9,39779,38981),
('Tennessee','F',10,40172,38970), ('Tennessee','F',11,39320,38800),
('Tennessee','F',12,37841,39457), ('Tennessee','F',13,37751,39966),
('Tennessee','F',14,38140,39992), ('Tennessee','F',15,38127,40397),
('Tennessee','F',16,37180,41672), ('Tennessee','F',17,37712,41922),
('Tennessee','F',18,38794,39486), ('Tennessee','F',19,41069,37269),
('Tennessee','F',20,40966,37313), ('Tennessee','F',21,39776,38036),
('Tennessee','F',22,38057,39511), ('Tennessee','F',23,37569,40316),
('Tennessee','F',24,36220,39746), ('Tennessee','F',25,38426,40139),
('Tennessee','F',26,37640,40682), ('Tennessee','F',27,38679,48581),
('Tennessee','F',28,40982,46327), ('Tennessee','F',29,43251,45846),
('Tennessee','F',30,43104,41500), ('Tennessee','F',31,40739,40367),
('Tennessee','F',32,39868,38349), ('Tennessee','F',33,40149,40812),
('Tennessee','F',34,41999,39743), ('Tennessee','F',35,45170,40560),
('Tennessee','F',36,46575,43390), ('Tennessee','F',37,45720,45311),
('Tennessee','F',38,45539,45048), ('Tennessee','F',39,46228,42362),
('Tennessee','F',40,47199,41920), ('Tennessee','F',41,45660,41697),
('Tennessee','F',42,45959,43766), ('Tennessee','F',43,46308,46960),
('Tennessee','F',44,44914,48801), ('Tennessee','F',45,45282,47552),
('Tennessee','F',46,43943,47340), ('Tennessee','F',47,42004,48266),
('Tennessee','F',48,41435,48205), ('Tennessee','F',49,39967,46881),
('Tennessee','F',50,40364,47040), ('Tennessee','F',51,40180,47277),
('Tennessee','F',52,39317,45529), ('Tennessee','F',53,41528,46180),
('Tennessee','F',54,31381,44780), ('Tennessee','F',55,31647,42574),
('Tennessee','F',56,31434,42183), ('Tennessee','F',57,32390,40796),
('Tennessee','F',58,29319,40796), ('Tennessee','F',59,27788,40763),
('Tennessee','F',60,27227,39895), ('Tennessee','F',61,25572,41149),
('Tennessee','F',62,25492,31308), ('Tennessee','F',63,23652,31296),
('Tennessee','F',64,24523,30992), ('Tennessee','F',65,23950,31582),
('Tennessee','F',66,22335,28314), ('Tennessee','F',67,21949,26397),
('Tennessee','F',68,21769,25723), ('Tennessee','F',69,21193,23869),
('Tennessee','F',70,21519,23428), ('Tennessee','F',71,19993,21436),
('Tennessee','F',72,20348,21942), ('Tennessee','F',73,19593,21182),
('Tennessee','F',74,19521,19373), ('Tennessee','F',75,19333,18833),
('Tennessee','F',76,18527,18385), ('Tennessee','F',77,17214,17593),
('Tennessee','F',78,17079,17676), ('Tennessee','F',79,15985,15877),
('Tennessee','F',80,14932,15899), ('Tennessee','F',81,13372,14740),
('Tennessee','F',82,12072,14145), ('Tennessee','F',83,11141,13431),
('Tennessee','F',84,10501,12493), ('Tennessee','F',85,60206,73831),
('Vermont','M',0,3358,3365), ('Vermont','M',1,3435,3464),
('Vermont','M',2,3378,3125), ('Vermont','M',3,3538,3359),
('Vermont','M',4,3710,3468), ('Vermont','M',5,3924,3457),
('Vermont','M',6,4038,3312), ('Vermont','M',7,4143,3459),
('Vermont','M',8,4212,3491), ('Vermont','M',9,4567,3564),
('Vermont','M',10,4705,3475), ('Vermont','M',11,4678,3608),
('Vermont','M',12,4734,3757), ('Vermont','M',13,4714,3972),
('Vermont','M',14,4645,4104), ('Vermont','M',15,4775,4219),
('Vermont','M',16,4622,4283), ('Vermont','M',17,4518,4682),
('Vermont','M',18,4687,4882), ('Vermont','M',19,5028,5003),
('Vermont','M',20,4890,4989), ('Vermont','M',21,4282,4717),
('Vermont','M',22,3753,4207), ('Vermont','M',23,3438,4054),
('Vermont','M',24,3128,3678), ('Vermont','M',25,3176,3639),
('Vermont','M',26,3122,3453), ('Vermont','M',27,3222,3827),
('Vermont','M',28,3508,3991), ('Vermont','M',29,3830,3497),
('Vermont','M',30,4051,3515), ('Vermont','M',31,3827,3406),
('Vermont','M',32,3681,3297), ('Vermont','M',33,3907,3389),
('Vermont','M',34,4188,3373), ('Vermont','M',35,4466,3451),
('Vermont','M',36,4720,3755), ('Vermont','M',37,4774,4107),
('Vermont','M',38,4808,4238), ('Vermont','M',39,5126,3973),
('Vermont','M',40,5145,3970), ('Vermont','M',41,5175,3979),
('Vermont','M',42,5174,4359), ('Vermont','M',43,5133,4596),
('Vermont','M',44,5134,4952), ('Vermont','M',45,5177,4944),
('Vermont','M',46,5056,4923), ('Vermont','M',47,4906,5284),
('Vermont','M',48,4777,5086), ('Vermont','M',49,4740,5223),
('Vermont','M',50,4687,5252), ('Vermont','M',51,4701,5120),
('Vermont','M',52,4631,5085), ('Vermont','M',53,4681,5111),
('Vermont','M',54,3421,5041), ('Vermont','M',55,3493,4893),
('Vermont','M',56,3455,4753), ('Vermont','M',57,3644,4661),
('Vermont','M',58,2940,4522), ('Vermont','M',59,2814,4511),
('Vermont','M',60,2626,4455), ('Vermont','M',61,2455,4498),
('Vermont','M',62,2472,3208), ('Vermont','M',63,2225,3253),
('Vermont','M',64,2202,3201), ('Vermont','M',65,2172,3379),
('Vermont','M',66,2013,2670), ('Vermont','M',67,1946,2478),
('Vermont','M',68,1954,2306), ('Vermont','M',69,1941,2085),
('Vermont','M',70,1915,2096), ('Vermont','M',71,1751,1845),
('Vermont','M',72,1870,1790), ('Vermont','M',73,1723,1725),
('Vermont','M',74,1550,1567), ('Vermont','M',75,1557,1495),
('Vermont','M',76,1402,1434), ('Vermont','M',77,1332,1380),
('Vermont','M',78,1180,1334), ('Vermont','M',79,1182,1169),
('Vermont','M',80,1014,1227), ('Vermont','M',81,887,1112),
('Vermont','M',82,841,935), ('Vermont','M',83,724,946),
('Vermont','M',84,594,810), ('Vermont','M',85,2814,3870),
('Vermont','F',0,3071,3223), ('Vermont','F',1,3290,3259),
('Vermont','F',2,3299,3066), ('Vermont','F',3,3408,3107),
('Vermont','F',4,3457,3199), ('Vermont','F',5,3803,3165),
('Vermont','F',6,3854,3189), ('Vermont','F',7,3907,3331),
('Vermont','F',8,4183,3168), ('Vermont','F',9,4248,3381),
('Vermont','F',10,4435,3378), ('Vermont','F',11,4438,3493),
('Vermont','F',12,4375,3486), ('Vermont','F',13,4385,3852),
('Vermont','F',14,4394,3933), ('Vermont','F',15,4391,3959),
('Vermont','F',16,4396,4260), ('Vermont','F',17,4436,4317),
('Vermont','F',18,4387,4701), ('Vermont','F',19,4655,4923),
('Vermont','F',20,4406,4741), ('Vermont','F',21,4135,4497),
('Vermont','F',22,3660,4112), ('Vermont','F',23,3271,3640),
('Vermont','F',24,3123,3477), ('Vermont','F',25,3247,3681),
('Vermont','F',26,3129,3515), ('Vermont','F',27,3220,3829),
('Vermont','F',28,3564,3482), ('Vermont','F',29,3861,3579),
('Vermont','F',30,4100,3356), ('Vermont','F',31,4021,3487),
('Vermont','F',32,3978,3345), ('Vermont','F',33,4198,3583),
('Vermont','F',34,4333,3392), ('Vermont','F',35,4663,3464),
('Vermont','F',36,4947,3703), ('Vermont','F',37,4999,4130),
('Vermont','F',38,5205,4292), ('Vermont','F',39,5361,4305),
('Vermont','F',40,5327,4185), ('Vermont','F',41,5301,4304),
('Vermont','F',42,5421,4590), ('Vermont','F',43,5559,4846),
('Vermont','F',44,5181,5122), ('Vermont','F',45,5365,5073),
('Vermont','F',46,5157,5231), ('Vermont','F',47,5134,5410),
('Vermont','F',48,5085,5295), ('Vermont','F',49,4951,5339),
('Vermont','F',50,4668,5413), ('Vermont','F',51,4620,5525),
('Vermont','F',52,4415,5079), ('Vermont','F',53,4738,5386),
('Vermont','F',54,3529,5104), ('Vermont','F',55,3553,4991),
('Vermont','F',56,3373,4951), ('Vermont','F',57,3551,4870),
('Vermont','F',58,3051,4462), ('Vermont','F',59,2911,4456),
('Vermont','F',60,2679,4306), ('Vermont','F',61,2496,4484),
('Vermont','F',62,2543,3413), ('Vermont','F',63,2392,3438),
('Vermont','F',64,2332,3195), ('Vermont','F',65,2326,3341),
('Vermont','F',66,2061,2801), ('Vermont','F',67,2208,2723),
('Vermont','F',68,2228,2466), ('Vermont','F',69,2246,2266),
('Vermont','F',70,2268,2256), ('Vermont','F',71,2167,2126),
('Vermont','F',72,2161,2072), ('Vermont','F',73,2023,2029),
('Vermont','F',74,2127,1780), ('Vermont','F',75,2004,1832),
('Vermont','F',76,2007,1877), ('Vermont','F',77,1904,1849),
('Vermont','F',78,1727,1819), ('Vermont','F',79,1683,1700),
('Vermont','F',80,1622,1658), ('Vermont','F',81,1414,1509),
('Vermont','F',82,1490,1569), ('Vermont','F',83,1245,1432),
('Vermont','F',84,1172,1397), ('Vermont','F',85,7300,8494);
"""
)

oven.engine.execute(
    """CREATE TABLE IF NOT EXISTS state_fact
(id text, name text, abbreviation text, country
text, type text, sort text, status text, occupied text, notes text,
fips_state text, assoc_press text, standard_federal_region text,
census_region text, census_region_name text, census_division text,
census_division_name text, circuit_court text);"""
)

oven.engine.execute(
    """insert into state_fact VALUES
('42','Tennessee','TN','USA','state','10','current','occupied','','47',
 'Tenn.','IV','3','South','6','East South Central','6'),
('45','Vermont','VT','USA','state','10','current','occupied','','50','Vt.',
 'I','1','Northeast','1','New England','2');"""
)


class MyTable(Base):
    first = Column("first", String(), primary_key=True)
    last = Column("last", String())
    age = Column("age", Integer())
    birth_date = Column("birth_date", Date())
    dt = Column("dt", DateTime())

    __tablename__ = "foo"
    __table_args__ = {"extend_existing": True}


class Scores(Base):
    username = Column("username", String(), primary_key=True)
    department = Column("department", String())
    testid = Column("testid", String())
    score = Column("score", Float())
    test_date = Column("test_date", Date())

    __tablename__ = "scores"
    __table_args__ = {"extend_existing": True}


DataTypesTable = Table(
    "datatypes", Base.metadata, autoload=True, autoload_with=oven.engine
)


class DataTypeser(Base):
    username = Column("username", String(), primary_key=True)
    department = Column("department", String())
    testid = Column("testid", String())
    score = Column("score", Float())
    test_date = Column("test_date", Date())
    test_datetime = Column("test_datetime", DateTime())

    __tablename__ = "datatypes"
    __table_args__ = {"extend_existing": True}


class ScoresWithNulls(Base):
    username = Column("username", String(), primary_key=True)
    department = Column("department", String())
    testid = Column("testid", String())
    score = Column("score", Float())
    test_date = Column("test_date", Date())

    __tablename__ = "scores_with_nulls"
    __table_args__ = {"extend_existing": True}


class WeirdTableWithColumnNamedTrue(Base):
    true_ = Column("true", String(), primary_key=True)

    __tablename__ = "weird_table_with_column_named_true"
    __table_args__ = {"extend_existing": True}


class TagScores(Base):
    username = Column("username", String(), primary_key=True)
    department = Column("department", String())
    tag = Column("tag", String())
    testid = Column("testid", String())
    score = Column("score", Float())

    __tablename__ = "tagscores"
    __table_args__ = {"extend_existing": True}


class Census(Base):
    state = Column("state", String(), primary_key=True)
    sex = Column("sex", String())
    age = Column("age", Integer())
    pop2000 = Column("pop2000", Integer())
    pop2008 = Column("pop2008", Integer())

    __tablename__ = "census"
    __table_args__ = {"extend_existing": True}


class DateTester(Base):
    dt = Column("dt", Date(), primary_key=True)
    count = Column("count", Integer())
    __tablename__ = "datetester"
    __table_args__ = {"extend_existing": True}


class StateFact(Base):
    id = Column("id", String(), primary_key=True)
    name = Column("name", String())
    abbreviation = Column("abbreviation", String())
    country = Column("country", String())
    type = Column("type", String())
    sort = Column("sort", String())
    status = Column("status", String())
    occupied = Column("occupied", String())
    notes = Column("notes", String())
    fips_state = Column("fips_state", String())
    assoc_press = Column("assoc_press", String())
    standard_federal_region = Column("standard_federal_region", String())
    census_region = Column("census_region", String())
    census_region_name = Column("census_region_name", String())
    census_division = Column("census_division", String())
    census_division_name = Column("census_division_name", String())
    circuit_court = Column("circuit_court", String())

    __tablename__ = "state_fact"
    __table_args__ = {"extend_existing": True}


mytable_shelf = Shelf(
    {
        "first": Dimension(MyTable.first),
        "last": Dimension(MyTable.last),
        "firstlast": Dimension(MyTable.last, id_expression=MyTable.first),
        "age": Metric(func.sum(MyTable.age)),
    }
)

mytable_extrarole_shelf = Shelf(
    {
        "first": Dimension(MyTable.first),
        "last": Dimension(MyTable.last),
        "firstlastage": Dimension(
            MyTable.last, id_expression=MyTable.first, age_expression=MyTable.age
        ),
        "age": Metric(func.sum(MyTable.age)),
    }
)

scores_shelf = Shelf(
    {
        "username": Dimension(Scores.username),
        "department": Dimension(
            Scores.department, anonymizer=lambda value: value[::-1] if value else "None"
        ),
        "testid": Dimension(Scores.testid),
        "test_cnt": Metric(func.count(distinct(TagScores.testid))),
        "score": Metric(func.avg(Scores.score)),
    }
)

tagscores_shelf = Shelf(
    {
        "username": Dimension(TagScores.username),
        "department": Dimension(TagScores.department),
        "testid": Dimension(TagScores.testid),
        "tag": Dimension(TagScores.tag),
        "test_cnt": Metric(func.count(distinct(TagScores.testid))),
        "score": Metric(func.avg(TagScores.score), summary_aggregation=func.sum),
    }
)

census_shelf = Shelf(
    {
        "state": Dimension(Census.state),
        "idvalue_state": IdValueDimension(Census.state, "State:" + Census.state),
        "sex": Dimension(Census.sex),
        "age": Dimension(Census.age),
        "pop2000": Metric(func.sum(Census.pop2000)),
        "pop2000_sum": Metric(func.sum(Census.pop2000), summary_aggregation=func.sum),
        "pop2008": Metric(func.sum(Census.pop2008)),
        "filter_all": Filter(1 == 0),
    }
)

statefact_shelf = Shelf(
    {
        "state": Dimension(StateFact.name),
        "abbreviation": Dimension(StateFact.abbreviation),
    }
)
