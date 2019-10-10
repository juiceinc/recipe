from recipe.oven.drivers.standard_oven import StandardOven


def test_create_standard_oven():
    oven = StandardOven("sqlite://")
    assert oven.engine.driver == "pysqlite"
    assert oven.Session.kw["bind"] == oven.engine


def test_create_standard_oven_no_target():
    oven = StandardOven()
    assert oven.engine is None
    assert oven.Session is None
