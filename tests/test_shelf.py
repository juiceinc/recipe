from copy import copy

import pytest
from sqlalchemy import join
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement
from yaml import safe_load

from recipe import (
    AutomaticShelf,
    BadIngredient,
    InvalidColumnError,
    BadRecipe,
    Dimension,
    Metric,
    Filter,
    Recipe,
    Shelf,
)
from recipe.ingredients import Ingredient, InvalidIngredient
from recipe.shelf import introspect_table
from recipe.schemas.utils import find_column

from .test_base import Base, Census, MyTable, StateFact, mytable_shelf, oven


class TestFindColumn(object):
    def test_find_column_from_recipe(self):
        """ Can find columns in a recipe. """
        content = """
        state:
            kind: Dimension
            field: state
        sex:
            kind: Dimension
            field: sex
        age:
            kind: Dimension
            field: age
        sum_pop2000:
            kind: Metric
            field: pop2000
        pop2008:
            kind: Metric
            field: pop2008
        ttlpop:
            kind: Metric
            field: pop2000 + pop2008

        """
        shelf = Shelf.from_validated_yaml(content, "census", metadata=Base.metadata)

        session = oven.Session()
        recipe = (
            Recipe(shelf=shelf, session=session)
            .metrics("sum_pop2000")
            .dimensions("state")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state,
       sum(census.pop2000) AS sum_pop2000
FROM census
GROUP BY state"""
        )

        col = find_column(recipe, "state")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        with pytest.raises(InvalidColumnError):
            find_column(recipe, "census_state")
        with pytest.raises(InvalidColumnError):
            find_column(recipe, "foo")

        col = find_column(recipe, "sum_pop2000")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        with pytest.raises(InvalidColumnError):
            find_column(recipe, "pop2000")

    def test_find_column_from_table_mytable(self):
        """SQLALchemy ORM Tables can be used and return
        InstrumentedAttributes"""
        col = find_column(MyTable, "first")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col2 = find_column(MyTable, "foo_first")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))
        assert col == col2

        col = find_column(MyTable, "last")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col = find_column(MyTable, "age")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col = find_column(MyTable, "foo_age")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        with pytest.raises(InvalidColumnError):
            find_column(MyTable, "foo")

    def test_find_column_from_join(self):
        """ Columns can be found in a join """
        j = join(Census, StateFact, Census.state == StateFact.name)

        # Names can be either the column name or the {tablename}_{column}
        col = find_column(j, "state")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col2 = find_column(j, "census_state")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))
        assert col == col2  # jingwei look into this

        # Names can be either the column name or the {tablename}_{column}
        col = find_column(j, "sex")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        col2 = find_column(j, "census_sex")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))
        assert col == col2

        col = find_column(j, "assoc_press")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))

        # Columns can be referenced as {tablename}_{column}
        col2 = find_column(j, "state_fact_assoc_press")
        assert isinstance(col, (ColumnElement, InstrumentedAttribute))
        assert col == col2

        with pytest.raises(InvalidColumnError):
            find_column(j, "foo")

    def test_find_column_from_invalid_type(self):
        """ Columns can be found in a join """
        with pytest.raises(InvalidColumnError):
            find_column(1, "foo")
        with pytest.raises(InvalidColumnError):
            find_column(MyTable.first, "foo")


class TestShelfConstruction(object):
    def test_pass_some_metadata(self):
        shelf = Shelf(metadata={"a": "hello"})
        assert shelf.Meta.metadata["a"] == "hello"

    def test_Meta_is_not_shared(self):
        shelf = Shelf(metadata={"a": "hello"})
        shelf2 = Shelf(metadata={"b": "there"})
        assert shelf.Meta.metadata == {"a": "hello"}
        assert shelf2.Meta.metadata == {"b": "there"}


class TestShelf(object):
    def setup(self):
        self.shelf = copy(mytable_shelf)

    def test_find(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.find("first", Dimension)
        assert ingredient.id == "first"

        # Raise if the wrong type
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("first", Metric)

        # Raise if key not present in shelf
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        # Raise if key is not an ingredient or string
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        self.shelf["foo"] = Dimension(MyTable.last)
        ingredient = self.shelf.find("last", Dimension)
        assert ingredient.id == "last"

    def test_find_filter(self):
        self.shelf["age_gt_20"] = Filter(MyTable.age > 20)

        ingredient = self.shelf.find("age_gt_20", Filter)
        assert ingredient.id == "age_gt_20"

        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("age_gt_20", Dimension)

    def test_repr(self):
        """ Find ingredients on the shelf """
        assert (
            self.shelf.__repr__()
            == """(Dimension)first MyTable.first
(Dimension)firstlast MyTable.first MyTable.last
(Dimension)last MyTable.last
(Metric)age sum(foo.age)"""
        )

    def test_keys(self):
        assert sorted(self.shelf.keys()) == ["age", "first", "firstlast", "last"]

    def test_update(self):
        """ Shelves can be updated with other shelves """
        new_shelf = Shelf({"squee": Dimension(MyTable.first)})
        assert len(self.shelf) == 4
        self.shelf.update(new_shelf)
        assert len(self.shelf) == 5

    def test_update_key_value(self):
        """ Shelves can be built with key_values and updated """
        new_shelf = Shelf(squee=Dimension(MyTable.first))
        assert len(self.shelf) == 4
        self.shelf.update(new_shelf)
        assert len(self.shelf) == 5
        assert isinstance(self.shelf.get("squee"), Dimension)

    def test_update_key_value_direct(self):
        """ Shelves can be updated directly with key_value"""
        assert len(self.shelf) == 4
        self.shelf.update(squee=Dimension(MyTable.first))
        assert len(self.shelf) == 5
        assert isinstance(self.shelf.get("squee"), Dimension)

    def test_brew(self):
        recipe_parts = self.shelf.brew_query_parts()
        assert len(recipe_parts["columns"]) == 5
        assert len(recipe_parts["group_bys"]) == 4
        assert len(recipe_parts["filters"]) == 0
        assert len(recipe_parts["havings"]) == 0

    def test_anonymize(self):
        """ We can save and store anonymization context """
        assert self.shelf.Meta.anonymize is False
        self.shelf.Meta.anonymize = True
        assert self.shelf.Meta.anonymize is True

    def test_get(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf["first"]
        assert ingredient.id == "first"

        ingredient = self.shelf.get("first", None)
        assert ingredient.id == "first"

        ingredient = self.shelf.get("primo", None)
        assert ingredient is None

    def test_get_doesnt_mutate(self):
        """
        Sharing ingredients between shelves won't cause race conditions on
        their `.id` and `.anonymize` attributes.
        """
        ingr = Ingredient(id="b")
        shelf = Shelf({"a": ingr})
        assert shelf["a"].id == "a"
        assert ingr.id == "b"

    def test_add_to_shelf(self):
        """ We can add an ingredient to a shelf """
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        self.shelf["foo"] = Dimension(MyTable.last)
        ingredient = self.shelf.find("last", Dimension)
        assert ingredient.id == "last"

    def test_setitem_type_error(self):
        """Only ingredients can be added to shelves."""
        with pytest.raises(TypeError):
            self.shelf["foo"] = 3

    def test_use_type_error(self):
        """`use` requires Ingredients"""
        with pytest.raises(TypeError):
            self.shelf.use(3)

    def test_clear(self):
        assert len(self.shelf) == 4
        self.shelf.clear()
        assert len(self.shelf) == 0

    def test_dimension_ids(self):
        assert len(self.shelf.dimension_ids) == 3
        assert sorted(self.shelf.dimension_ids) == ["first", "firstlast", "last"]

    def test_metric_ids(self):
        assert len(self.shelf.metric_ids) == 1
        assert self.shelf.metric_ids == ("age",)

    def test_filter_ids(self):
        assert len(self.shelf.filter_ids) == 0


class TestShelfFromYaml(object):
    def make_shelf(self, content, table=MyTable):
        self.shelf = Shelf.from_yaml(content, table)
        self.shelf.Meta.anonymize = False

    def setup(self):
        self.make_shelf(
            """
first:
    kind: Dimension
    field: first
last:
    kind: Dimension
    field: last
age:
    kind: Metric
    field: age
"""
        )

    def test_find(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.find("first", Dimension)
        assert ingredient.id == "first"

        # Raise if the wrong type
        with pytest.raises(Exception):
            ingredient = self.shelf.find("first", Metric)

        # Raise if key not present in shelf
        with pytest.raises(Exception):
            ingredient = self.shelf.find("foo", Dimension)

        # Raise if key is not an ingredient or string
        with pytest.raises(Exception):
            ingredient = self.shelf.find(2.0, Dimension)

        with pytest.raises(Exception):
            ingredient = self.shelf.find("foo", Dimension)

        with pytest.raises(Exception):
            ingredient = self.shelf.find(2.0, Dimension)

        with pytest.raises(Exception):
            ingredient = self.shelf.find("foo", Dimension)

        with pytest.raises(Exception):
            ingredient = self.shelf.find("foo", Dimension)

        self.shelf["foo"] = Dimension(MyTable.last)
        ingredient = self.shelf.find("last", Dimension)
        assert ingredient.id == "last"

    def test_repr(self):
        """ Find ingredients on the shelf """
        assert (
            self.shelf.__repr__()
            == """(Dimension)first MyTable.first
(Dimension)last MyTable.last
(Metric)age sum(foo.age)"""
        )

    def test_update(self):
        """ Shelves can be updated with other shelves """
        new_shelf = Shelf({"squee": Dimension(MyTable.first)})
        assert len(self.shelf) == 3
        self.shelf.update(new_shelf)
        assert len(self.shelf) == 4

    def test_update_key_value(self):
        """ Shelves can be built with key_values and updated """
        new_shelf = Shelf(squee=Dimension(MyTable.first))
        assert len(self.shelf) == 3
        self.shelf.update(new_shelf)
        assert len(self.shelf) == 4
        assert isinstance(self.shelf.get("squee"), Dimension)

    def test_update_key_value_direct(self):
        """ Shelves can be updated directly with key_value"""
        assert len(self.shelf) == 3
        self.shelf.update(squee=Dimension(MyTable.first))
        assert len(self.shelf) == 4
        assert isinstance(self.shelf.get("squee"), Dimension)

    def test_brew(self):
        recipe_parts = self.shelf.brew_query_parts()
        assert len(recipe_parts["columns"]) == 3
        assert len(recipe_parts["group_bys"]) == 2
        assert len(recipe_parts["filters"]) == 0
        assert len(recipe_parts["havings"]) == 0

    def test_anonymize(self):
        """ We can save and store anonymization context """
        assert self.shelf.Meta.anonymize is False
        self.shelf.Meta.anonymize = True
        assert self.shelf.Meta.anonymize is True

    def test_anonymize_keeps_ingredients_up_to_date(self):
        """Setting the anonymize attribute causes all ingredients to be
        updated.
        """
        assert self.shelf["first"].anonymize is False
        self.shelf.Meta.anonymize = True
        assert self.shelf["first"].anonymize is True

    def test_get(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf["first"]
        assert ingredient.id == "first"

        ingredient = self.shelf.get("first", None)
        assert ingredient.id == "first"

        ingredient = self.shelf.get("primo", None)
        assert ingredient is None

    def test_add_to_shelf(self):
        """ We can add an ingredient to a shelf """
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        self.shelf["foo"] = Dimension(MyTable.last)
        ingredient = self.shelf.find("last", Dimension)
        assert ingredient.id == "last"

    def test_clear(self):
        assert len(self.shelf) == 3
        self.shelf.clear()
        assert len(self.shelf) == 0

    def test_scalars_in_condition(self):
        for condition in ("gt", "gte", "lt", "lte", "eq", "ne", "foo"):
            content = """
oldage:
    kind: Metric
    field:
        value: age
        condition:
            field: age
            {}: [24,42]
""".format(
                condition
            )
            with pytest.raises(Exception):
                self.make_shelf(content)

    def test_conditions(self):
        for condition, symbl in (
            ("gt", ">"),
            ("gte", ">="),
            ("lt", "<"),
            ("lte", "<="),
            ("eq", "="),
            ("ne", "!="),
        ):
            content = """
oldage:
    kind: Metric
    field:
        value: age
        condition:
            field: age
            {}: 40
""".format(
                condition
            )
            self.make_shelf(content)
            assert str(
                self.shelf["oldage"]
            ) == "(Metric)oldage sum(CASE WHEN (foo.age {} ?) THEN foo.age " "END)".format(
                symbl
            )
            assert isinstance(self.shelf["oldage"], Metric)

    def test_invalid_kind(self):
        content = """
oldage:
    kind: Metric2
    field:
        value: age
"""
        with pytest.raises(Exception):
            self.make_shelf(content)

    def test_invalid_condition(self):
        content = """
oldage:
    kind: Metric
    field:
        value: age
        condition: 14
"""
        with pytest.raises(Exception):
            self.make_shelf(content)

    def test_missing_condition(self):
        content = """
oldage:
    kind: Metric
    field:
        value: age
"""
        self.make_shelf(content)
        # null conditions are ignored.
        assert str(self.shelf["oldage"]) == "(Metric)oldage sum(foo.age)"

    def test_null_aggregation(self):
        content = """
oldage:
    kind: Metric
    field:
        value: age
        aggregation: null
"""
        self.make_shelf(content)
        # null conditions are ignored.
        assert str(self.shelf["oldage"]) == "(Metric)oldage MyTable.age"

    def test_invalid_aggregations(self):
        for aggr in (24, 1.0, "foo"):
            content = """
oldage:
    kind: Metric
    field:
        value: age
        aggregation: {}
""".format(
                aggr
            )
        with pytest.raises(Exception):
            self.make_shelf(content)

    def test_field_without_value(self):
        content = """
oldage:
    kind: Metric
    field:
        value: null
        aggregation: sum
"""
        with pytest.raises(Exception):
            self.make_shelf(content)


class TestShelfFromValidatedYaml(TestShelfFromYaml):
    """Test that shelves are created correctly using
    sureberus validation.
    """

    def make_shelf(self, content, table=MyTable):
        self.shelf = Shelf.from_validated_yaml(content, table)
        self.shelf.Meta.anonymize = False

    def test_null_condition(self):
        """sureberus validated shelf doesn't accept null."""
        content = """
oldage:
    kind: Metric
    field:
        value: age
        condition: null
"""
        with pytest.raises(Exception):
            self.make_shelf(content)

    def test_null_aggregation(self):
        content = """
oldage:
    kind: Metric
    field:
        value: age
        aggregation: null
"""
        self.make_shelf(content)
        # Explicit null aggregations are respected, even in metrics
        assert str(self.shelf["oldage"]) == "(Metric)oldage MyTable.age"

    def test_invalid_column(self):
        content = """
oldage:
    kind: Metric
    field:
        value: age
invalid:
    kind: Metric
    field: invalid
invalid_in_referred:
    kind: Metric
    field: age
    divide_by: '@invalid'
"""
        self.make_shelf(content)
        assert isinstance(self.shelf["oldage"], Metric)
        assert isinstance(self.shelf["invalid"], InvalidIngredient)
        assert self.shelf["invalid"].error["extra"]["column_name"] == "invalid"
        assert (
            self.shelf["invalid_in_referred"].error["extra"]["column_name"] == "invalid"
        )

    def test_invalid_definition(self):
        content = """
oldage:
    kind: Metric
    field:
        value: age
invalid:
    kind: Dimension
    field: age
    # raw_field is a reserved name and can't be assigned in config.
    raw_field: first
"""
        self.make_shelf(content)
        assert isinstance(self.shelf["oldage"], Metric)
        assert isinstance(self.shelf["invalid"], InvalidIngredient)
        assert (
            self.shelf["invalid"].error["extra"]["details"]
            == "raw is a reserved role in dimensions"
        )


class TestShelfFromConfig(TestShelfFromValidatedYaml):
    def make_shelf(self, content, table=MyTable):
        obj = safe_load(content)
        self.shelf = Shelf.from_config(obj, table)


class TestShelfFromIntrospection(object):
    """Test that shelves are created correctly using
    sureberus validation.
    """

    def test_shelf(self):
        """Cerberus validated shelf doesn't accept null."""
        content = """
state:
    kind: Dimension
    field: state
sex:
    kind: Dimension
    field: sex
age:
    kind: Dimension
    field: age
pop2000:
    kind: Metric
    field: pop2000
pop2008:
    kind: Metric
    field: pop2008
ttlpop:
    kind: Metric
    field: pop2000 + pop2008

"""
        shelf = Shelf.from_validated_yaml(content, "census", metadata=Base.metadata)

        session = oven.Session()
        recipe = (
            Recipe(shelf=shelf, session=session).metrics("pop2000").dimensions("state")
        )
        assert (
            recipe.to_sql()
            == """SELECT census.state AS state,
       sum(census.pop2000) AS pop2000
FROM census
GROUP BY state"""
        )
        assert (
            recipe.dataset.tsv
            == """state	pop2000	state_id\r
Tennessee	5685230	Tennessee\r
Vermont	609480	Vermont\r
"""
        )


class TestAutomaticShelf(object):
    def setup(self):
        self.shelf = AutomaticShelf(MyTable)

    def test_auto_find(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf.find("first", Dimension)
        assert ingredient.id == "first"

        # Raise if the wrong type
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("first", Metric)

        # Raise if key not present in shelf
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find("foo", Dimension)

        # Raise if key is not an ingredient or string
        with pytest.raises(BadRecipe):
            ingredient = self.shelf.find(2.0, Dimension)

    def test_get(self):
        """ Find ingredients on the shelf """
        ingredient = self.shelf["first"]
        assert ingredient.id == "first"

        ingredient = self.shelf.get("first", None)
        assert ingredient.id == "first"

        ingredient = self.shelf["age"]
        assert str(ingredient.columns[0]) == "sum(foo.age)"

        ingredient = self.shelf.get("primo", None)
        assert ingredient is None

    def test_introspect_table(self):
        config = introspect_table(MyTable.__table__)
        assert config == {
            "age": {"field": "age", "kind": "Metric"},
            "first": {"field": "first", "kind": "Dimension"},
            "last": {"field": "last", "kind": "Dimension"},
        }
