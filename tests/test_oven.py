from recipe.oven import get_oven


def test_create_oven():
    oven = get_oven("sqlite://")
    assert oven.engine.driver == "pysqlite"
    assert oven.Session.kw["bind"] == oven.engine


def test_create_oven_no_target():
    oven = get_oven()
    assert oven.engine is None
    assert oven.Session is None
