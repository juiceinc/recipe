from recipe.oven import Oven

def test_create_oven():
    oven = Oven('sqlite://')
    assert oven.engine.driver == 'pysqlite'
    assert oven.Session.kw['bind'] == oven.engine
