from recipe.oven import Oven


def test_create_oven():
    oven = Oven('sqlite://')
    assert oven.engine.driver == 'pysqlite'
    assert oven.Session.kw['bind'] == oven.engine


def test_create_oven_no_target():
    oven = Oven()
    assert oven.engine == None
    assert oven.Session == None
