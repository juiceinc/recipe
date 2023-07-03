"""Test the lark grammar used to define field expressions."""

import time
from functools import partial
from typing import Callable, Tuple, List, Optional

from freezegun import freeze_time
from sqlalchemy.ext.serializer import dumps, loads

from recipe.schemas.builders import SQLAlchemyBuilder
from recipe.schemas.expression_grammar import (
    gather_columns,
    is_valid_column,
    make_column_collection_for_selectable,
    make_columns_grammar,
)
from recipe.utils.formatting import expr_to_str
from tests.test_base import (
    RecipeTestCase,
    str_dedent,
    get_bigquery_connection_string,
    get_bigquery_engine_kwargs,
)

utc_offset = -1 * time.localtime().tm_gmtoff / 3600.0 + time.localtime().tm_isdst


class GrammarTestCase(RecipeTestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()
        extra_selectables = [(self.scores_table, "scores")]
        self.builder = SQLAlchemyBuilder.get_builder(
            self.datatypes_table,
            extra_selectables=extra_selectables,
            drivername=self.dbinfo.engine.url.drivername,
        )

    def examples(self, input_rows: str):
        """Take input where each line looks like
        field     -> expected_sql
        #field    -> expected_sql (commented out)
        field     -> expected_sql  engine=sqlite
        """
        for row in input_rows.split("\n"):
            row = row.strip()
            if row == "" or row.startswith("#"):
                continue

            if "->" in row:
                field, expected_sql = row.split("->")
            else:
                field = row
                expected_sql = "None"
            if expected_sql:
                pass
            expected_sql = expected_sql.strip()
            yield field, expected_sql

    def _parse_examples(self, input_rows: str, split_on: str = "\n"):
        """
        Parse examples taken from a multiline string

        field     -> expected_sql
        #field    -> expected_sql (commented out)
        # The following example only runs on sqlite
        sqlite::field     -> expected_sql
        """
        for row in input_rows.split(split_on):
            row = row.strip()
            if row == "" or row.startswith("#"):
                continue
            if "->" not in row:
                continue
            field, expected_value = row.split("->", 1)
            field = field.strip()
            expected_value = expected_value.strip()
            yield field, expected_value

    def validate_examples(
        self,
        input_rows: str,
        constructor: Callable[[str], Tuple[str, str]],
        include_drivernames: Optional[List[str]] = None,
        exclude_drivernames: Optional[List[str]] = None,
    ):
        """
        Test sql generation with examples taken from a string.

        field     -> expected_sql
        #field    -> expected_sql (commented out)
        # The following example only runs on sqlite
        sqlite::field     -> expected_sql
        """
        if self._skip_drivername(include_drivernames, exclude_drivernames):
            return

        for idx, (field, expected_sql) in enumerate(
            self._parse_examples(input_rows=input_rows)
        ):
            generated_expr, generated_dtype = constructor(field, debug=False)
            generated_sql = expr_to_str(generated_expr, engine=self.dbinfo.engine)
            if self.drivername.startswith("bigquery"):
                generated_sql = generated_sql.replace("`", "")
            if generated_sql != expected_sql:
                print(
                    f"""Field '{field.strip()}' with drivername '{self.builder.drivername}' failed.
    Expected: {expected_sql}
    Actual:   {generated_sql}
                    """
                )
            self.assertEqual(generated_sql, expected_sql)

    def validate_examples_data_type(
        self,
        input_rows: str,
        constructor: Callable[[str], Tuple[str, str]],
        include_drivernames: Optional[List[str]] = None,
        exclude_drivernames: Optional[List[str]] = None,
    ):
        """
        Test sql generation with examples taken from a string.

        field     -> expected_sql
        #field    -> expected_sql (commented out)
        # The following example only runs on sqlite
        sqlite::field     -> expected_sql
        """
        if self._skip_drivername(include_drivernames, exclude_drivernames):
            return

        for field, expected_datatype in self._parse_examples(input_rows=input_rows):
            generated_expr, generated_dtype = constructor(field, debug=False)
            if generated_dtype != expected_datatype:
                print(
                    f"""Field '{field.strip()}'
    Expected: {expected_datatype}
    Actual:   {generated_dtype}
"""
                )
            self.assertEqual(expected_datatype, generated_dtype)

    def validate_bad_examples(
        self,
        input_rows: str,
        constructor: Callable[[str], Tuple[str, str]],
        include_drivernames: Optional[List[str]] = None,
        exclude_drivernames: Optional[List[str]] = None,
    ):
        """Take input where each input is separated by three equals

        field ->
        expected_error
        ===
        field ->
        expected_error
        ===
        #field ->
        expected_error  (commented out)

        """
        if self._skip_drivername(include_drivernames, exclude_drivernames):
            return
        for field, expected_error in self._parse_examples(
            input_rows=input_rows, split_on="==="
        ):
            with self.assertRaises(Exception) as e:
                constructor(field, debug=False)

            if str(e.exception).strip() != expected_error.strip():
                print(
                    f"""Field '{field.strip()}'
----
Expected error:
{expected_error}
----
Actual error:
{e.exception}
"""
                )
            self.assertEqual(str(e.exception).strip(), expected_error.strip())


class DrivernameTestCase(GrammarTestCase):
    def test_drivername(self):
        self.assertEqual(self.drivername, "sqlite")


class PostgresDrivernameTestCase(GrammarTestCase):
    connection_string = "postgresql+psycopg2://postgres:postgres@db:5432/postgres"

    def test_drivername(self):
        self.assertEqual(self.drivername, "postgresql+psycopg2")


class BigqueryDrivernameTestCase(GrammarTestCase):
    connection_string = get_bigquery_connection_string()
    engine_kwargs = get_bigquery_engine_kwargs()

    def test_drivername(self):
        self.assertEqual(self.drivername, "bigquery")


class BuildGrammarTestCase(RecipeTestCase):
    def setUp(self):
        super().setUp()

        self.selectables = [
            self.basic_table,
            self.datatypes_table,
            self.recipe_from_config(
                {"dimensions": ["first", "last"], "metrics": ["age"]}
            ),
            self.recipe_from_config({"dimensions": ["firstlast"], "metrics": ["age"]}),
        ]

    def assertSelectableGrammar(self, selectable, grammar_text: str, *, namespace=None):
        grammar = make_columns_grammar(
            make_column_collection_for_selectable(selectable, namespace=namespace)
        )

        if str_dedent(grammar) != str_dedent(grammar_text):
            print(
                f"Actual:\n{str_dedent(grammar)}\n\nExpected:\n{str_dedent(grammar_text)}"
            )
        self.assertEqual(str_dedent(grammar), str_dedent(grammar_text))

    def test_make_columns_for_table(self):
        expected_column_keys = [
            ["date", "datetime", "num", "str", "str"],
            ["bool", "date", "datetime", "num", "str", "str", "str"],
            ["num", "str", "str"],
            ["num", "str", "str"],
        ]

        for selectable, expected_column_keys in zip(
            self.selectables, expected_column_keys
        ):
            cc = make_column_collection_for_selectable(selectable)
            ctypes = sorted([col.datatype for col in cc.columns])
            self.assertEqual(ctypes, expected_column_keys)

        with self.assertRaises(Exception):
            make_column_collection_for_selectable(None)
        with self.assertRaises(Exception):
            make_column_collection_for_selectable("foo")

    def test_make_columns_grammar(self):
        expected_grammars = [
            """
            date_0: "[" + /birth_date/i + "]" | /birth_date/i
            datetime_0: "[" + /dt/i + "]" | /dt/i
            num_0: "[" + /age/i + "]" | /age/i
            str_0: "[" + /first/i + "]" | /first/i
            str_1: "[" + /last/i + "]" | /last/i
            """,
            """
            bool_0: "[" + /valid_score/i + "]" | /valid_score/i
            date_0: "[" + /test_date/i + "]" | /test_date/i
            datetime_0: "[" + /test_datetime/i + "]" | /test_datetime/i
            num_0: "[" + /score/i + "]" | /score/i
            str_0: "[" + /department/i + "]" | /department/i
            str_1: "[" + /testid/i + "]" | /testid/i
            str_2: "[" + /username/i + "]" | /username/i
            """,
            """
            num_0: "[" + /age/i + "]" | /age/i
            str_0: "[" + /first/i + "]" | /first/i
            str_1: "[" + /last/i + "]" | /last/i
            """,
            """
            num_0: "[" + /age/i + "]" | /age/i
            str_0: "[" + /firstlast/i + "]" | /firstlast/i
            str_1: "[" + /firstlast_id/i + "]" | /firstlast_id/i
            """,
        ]
        for selectable, expected_grammar in zip(self.selectables, expected_grammars):
            self.assertSelectableGrammar(selectable, expected_grammar)

    def test_make_columns_grammar_with_namespace(self):
        # If we pass a namespace, field names will be prefixed by the namespace
        expected_grammars = [
            """
            date_0: "[" + /foo\.birth_date/i + "]" | /foo\.birth_date/i
            datetime_0: "[" + /foo\.dt/i + "]" | /foo\.dt/i
            num_0: "[" + /foo\.age/i + "]" | /foo\.age/i
            str_0: "[" + /foo\.first/i + "]" | /foo\.first/i
            str_1: "[" + /foo\.last/i + "]" | /foo\.last/i
            """
        ]
        for selectable, expected_grammar in zip(self.selectables, expected_grammars):
            self.assertSelectableGrammar(selectable, expected_grammar, namespace="foo")

    def test_gather_columns(self):
        """Gathered columns collects all the rules for column types into a single rule"""
        expected_gathered_columns = [
            """
            unusable_col: "DUMMYVALUNUSABLECOL"
            date.1: date_0 | extra_date_rule | "(" + date + ")"
            datetime.2: datetime_0 | extra_datetime_rule | "(" + datetime + ")"
            datetime_end.1: datetime_0 | datetime_end_conv | datetime_aggr | "(" + datetime_end + ")"
            boolean.1: TRUE | FALSE | extra_bool_rule | "(" + boolean + ")"
            string.1: str_0 | str_1 | ESCAPED_STRING | extra_string_rule | "(" + string + ")"
            num.1: num_0 | NUMBER | extra_num_rule | "(" + num + ")"
            """,
            """
            unusable_col: "DUMMYVALUNUSABLECOL"
            date.1: date_0 | extra_date_rule | "(" + date + ")"
            datetime.2: datetime_0 | extra_datetime_rule | "(" + datetime + ")"
            datetime_end.1: datetime_0 | datetime_end_conv | datetime_aggr | "(" + datetime_end + ")"
            boolean.1: bool_0 | TRUE | FALSE | extra_bool_rule | "(" + boolean + ")"
            string.1: str_0 | str_1 | str_2 | ESCAPED_STRING | extra_string_rule | "(" + string + ")"
            num.1: num_0 | NUMBER | extra_num_rule | "(" + num + ")"
            """,
            """
            unusable_col: "DUMMYVALUNUSABLECOL"
            date.1: extra_date_rule | "(" + date + ")"
            datetime.2: extra_datetime_rule | "(" + datetime + ")"
            datetime_end.1: datetime_end_conv | datetime_aggr | "(" + datetime_end + ")"
            boolean.1: TRUE | FALSE | extra_bool_rule | "(" + boolean + ")"
            string.1: str_0 | str_1 | ESCAPED_STRING | extra_string_rule | "(" + string + ")"
            num.1: num_0 | NUMBER | extra_num_rule | "(" + num + ")"
            """,
            """
            unusable_col: "DUMMYVALUNUSABLECOL"
            date.1: extra_date_rule | "(" + date + ")"
            datetime.2: extra_datetime_rule | "(" + datetime + ")"
            datetime_end.1: datetime_end_conv | datetime_aggr | "(" + datetime_end + ")"
            boolean.1: TRUE | FALSE | extra_bool_rule | "(" + boolean + ")"
            string.1: str_0 | str_1 | ESCAPED_STRING | extra_string_rule | "(" + string + ")"
            num.1: num_0 | NUMBER | extra_num_rule | "(" + num + ")"
            """,
        ]
        for selectable, expected_gathered in zip(
            self.selectables, expected_gathered_columns
        ):
            columns = make_column_collection_for_selectable(selectable)
            gathered_columns = f"""
            {gather_columns("unusable_col", columns, "unusable")}
            {gather_columns("date.1", columns, "date", additional_rules=["extra_date_rule"])}
            {gather_columns("datetime.2", columns, "datetime", additional_rules=["extra_datetime_rule"])}
            {gather_columns("datetime_end.1", columns, "datetime", additional_rules=["datetime_end_conv", "datetime_aggr"])}
            {gather_columns("boolean.1", columns, "bool", additional_rules=["TRUE", "FALSE", "extra_bool_rule"])}
            {gather_columns("string.1", columns, "str", additional_rules=["ESCAPED_STRING", "extra_string_rule"])}
            {gather_columns("num.1", columns, "num", additional_rules=["NUMBER", "extra_num_rule"])}
            """
            self.assertEqual(
                str_dedent(gathered_columns), str_dedent(expected_gathered)
            )


class TestSQLAlchemyBuilder(GrammarTestCase):
    def test_enforce_aggregation(self):
        """Enforce aggregation will wrap the function in a sum if no aggregation was seen"""

        good_examples = """
        [score]                         -> sum(datatypes.score)
        [ScORE]                         -> sum(datatypes.score)
        [ScORE] + [ScORE]               -> sum(datatypes.score + datatypes.score)
        max([ScORE] + [ScORE])          -> max(datatypes.score + datatypes.score)
        max(score) - min(score)         -> max(datatypes.score) - min(datatypes.score)
        max(scores.score)               -> max(scores.score)
        max([score] - [scores.score])   -> max(datatypes.score - scores.score)
        """

        self.validate_examples(
            good_examples, partial(self.builder.parse, enforce_aggregation=True)
        )

    def test_data_type(self):
        good_examples = """
        [score]                         -> num
        [ScORE]                         -> num
        [ScORE] + [ScORE]               -> num
        max([ScORE] + [ScORE])          -> num
        max(score) - min(score)         -> num
        department                      -> str
        department > "foo"              -> bool
        day(test_date)                  -> date
        month(test_datetime)            -> date
        department > "foo" anD [score] < 22    -> bool
        min(department)                 -> str
        min(test_date)                  -> date
        count(*)                        -> num
        count(department > "foo")       -> num
        substr(department, 5)           -> str
        substr(department, 5, 5)        -> str
        max([score] - [scores.score])   -> num
        """
        self.validate_examples_data_type(good_examples, partial(self.builder.parse))

    def test_literals(self):
        examples = """
        "22"          -> str
        2.0           -> num
        2.0 + 1.0     -> num
        "220" + "foo" -> str
        5             -> num
        """
        self.validate_examples_data_type(examples, partial(self.builder.parse))

    def test_selectable_recipe(self):
        """Test a selectable that is a recipe"""
        recipe = (
            self.recipe(shelf=self.mytable_shelf).metrics("age").dimensions("first")
        )
        b = SQLAlchemyBuilder(selectable=recipe)
        type_examples = """
        [age]                         -> num
        [first]                       -> str
        [first] > "foo"               -> bool
        age * 2                       -> num
        """
        self.validate_examples_data_type(type_examples, partial(b.parse))

        sql_examples = """
        [age]                         -> anon_1.age
        [first]                       -> anon_1.first
        [first] > "foo"               -> anon_1.first > 'foo'
        age * 2                       -> anon_1.age * 2
        """
        self.validate_examples(
            sql_examples, partial(b.parse), exclude_drivernames=["bigquery"]
        )

    def test_selectable_orm(self):
        """Test a selectable that is a orm class"""
        b = SQLAlchemyBuilder(selectable=self.datatypes_table)
        type_examples = """
        [score]                         -> num
        score                           -> num
        [testid]                        -> str
        [username] > "foo"              -> bool
        score * 2                       -> num
        test_date                       -> date
        """
        self.validate_examples_data_type(type_examples, partial(b.parse))

        sql_examples = """
        [score]                         -> datatypes.score
        [testid]                        -> datatypes.testid
        [username] > "foo"              -> datatypes.username > 'foo'
        score * 2                       -> datatypes.score * 2
        test_date                       -> datatypes.test_date
        """
        self.validate_examples(
            sql_examples, partial(b.parse), exclude_drivernames=["bigquery"]
        )

    def test_selectable_census(self):
        """Test a selectable that is a orm class"""
        b = SQLAlchemyBuilder(selectable=self.census_table)
        type_examples = """
        age                             -> num
        state                           -> str
        [pop2000] + pop2008             -> num
        state + sex                     -> str
        state = "2"                     -> bool
        max(pop2000) > 100              -> bool
        """
        self.validate_examples_data_type(type_examples, partial(b.parse))

        sql_examples = """
        age                             -> census.age
        state                           -> census.state
        min([pop2000] + pop2008)        -> min(census.pop2000 + census.pop2008)
        state + sex                     -> census.state || census.sex
        """
        self.validate_examples(
            sql_examples, partial(b.parse), exclude_drivernames=["bigquery"]
        )

        sql_examples = """
        age                             -> age
        state                           -> state
        min([pop2000] + pop2008)        -> min(census.pop2000 + census.pop2008)
        state + sex                     -> census.state || census.sex
        """
        self.validate_examples(
            sql_examples, partial(b.parse), include_drivernames=["bigquery"]
        )


class TestSQLAlchemyBuilderPostgres(TestSQLAlchemyBuilder):
    connection_string = "postgresql+psycopg2://postgres:postgres@db:5432/postgres"


class TestSQLAlchemyBuilderBigquery(TestSQLAlchemyBuilder):
    connection_string = get_bigquery_connection_string()
    engine_kwargs = get_bigquery_engine_kwargs()


class TestSQLAlchemyBuilderConvertDates(GrammarTestCase):
    def test_enforce_convert_dates(self):
        """Enforce aggregation will wrap the function in a sum if no aggregation was seen"""

        good_examples = """
        [test_date]                       -> date_trunc('year', datatypes.test_date)
        test_date                         -> date_trunc('year', datatypes.test_date)
        coalesce([test_date], date("2020-01-01"))   -> coalesce(date_trunc('year', datatypes.test_date), '2020-01-01')
        """
        self.validate_examples(
            good_examples,
            partial(
                self.builder.parse,
                enforce_aggregation=True,
                convert_dates_with="year_conv",
            ),
            exclude_drivernames=["bigquery"],
        )
        bigquery_examples = """
        [test_date]                       -> date_trunc(datatypes.test_date, year)
        test_date                         -> date_trunc(datatypes.test_date, year)
        coalesce([test_date], date("2020-01-01"))   -> coalesce(date_trunc(datatypes.test_date, year), '2020-01-01')
        """
        self.validate_examples(
            bigquery_examples,
            partial(
                self.builder.parse,
                enforce_aggregation=True,
                convert_dates_with="year_conv",
            ),
            include_drivernames=["bigquery"],
        )

        good_examples = """
        [test_date]                       -> date_trunc('month', datatypes.test_date)
        test_date                         -> date_trunc('month', datatypes.test_date)
        coalesce([test_date], date("2020-01-01"))   -> coalesce(date_trunc('month', datatypes.test_date), '2020-01-01')
        """
        self.validate_examples(
            good_examples,
            partial(
                self.builder.parse,
                enforce_aggregation=True,
                convert_dates_with="month_conv",
            ),
            exclude_drivernames=["bigquery"],
        )
        bigquery_examples = """
        [test_date]                       -> date_trunc(datatypes.test_date, month)
        test_date                         -> date_trunc(datatypes.test_date, month)
        coalesce([test_date], date("2020-01-01"))   -> coalesce(date_trunc(datatypes.test_date, month), '2020-01-01')
        """
        self.validate_examples(
            bigquery_examples,
            partial(
                self.builder.parse,
                enforce_aggregation=True,
                convert_dates_with="month_conv",
            ),
            include_drivernames=["bigquery"],
        )

        # If the date conversion doesn't exist, don't convert
        good_examples = """
        [test_date]                       -> datatypes.test_date
        test_date                         -> datatypes.test_date
        coalesce([test_date], date("2020-01-01"))   -> coalesce(datatypes.test_date, '2020-01-01')
        """
        self.validate_examples(
            good_examples,
            partial(
                self.builder.parse,
                enforce_aggregation=True,
                convert_dates_with="a_potato",
            ),
            exclude_drivernames=["bigquery"],
        )


class TestSQLAlchemyBuilderConvertDatesPostgres(TestSQLAlchemyBuilderConvertDates):
    connection_string = "postgresql+psycopg2://postgres:postgres@db:5432/postgres"


class TestSQLAlchemyBuilderConvertDatesBigquery(TestSQLAlchemyBuilderConvertDates):
    connection_string = get_bigquery_connection_string()
    engine_kwargs = get_bigquery_engine_kwargs()


class TestDataTypesTable(GrammarTestCase):
    def test_fields_and_addition(self):
        """These examples should all succeed"""

        good_examples = """
        [ScORE] + [ScORE]               -> datatypes.score + datatypes.score
        [score] + 2.0                   -> datatypes.score + 2.0
        substr(department, 5)           -> substr(datatypes.department, 5)
        substr(department, 5, 2)        -> substr(datatypes.department, 5, 2)
        #([score] + 2.0) / [score]                   -> datatypes.score + 2.0
        [username] + [department]       -> datatypes.username || datatypes.department
        "foo" + [department]            -> 'foo' || datatypes.department
        1.0 + [score]                   -> 1.0 + datatypes.score
        1.0 + [score] + [score]         -> 1.0 + datatypes.score + datatypes.score
        -0.1 * [score] + 600            -> -0.1 * datatypes.score + 600
        -0.1 * [score] + 600.0          -> -0.1 * datatypes.score + 600.0
        [score] = [score]               -> datatypes.score = datatypes.score
        [score] >= 2.0                  -> datatypes.score >= 2.0
        2.0 <= [score]                  -> datatypes.score >= 2.0
        NOT [score] >= 2.0              -> datatypes.score < 2.0
        NOT 2.0 <= [score]              -> datatypes.score < 2.0
        [score] > 3 AND true            -> datatypes.score > 3
        # This is a bad case
        # what happens is TRUE AND score > 3 gets simplified to score > 3
        valid_score = TRUE AND score > 4 -> datatypes.valid_score = (datatypes.score > 4)
        [score] = Null                  -> datatypes.score IS NULL
        [score] IS NULL                 -> datatypes.score IS NULL
        [score] != Null                 -> datatypes.score IS NOT NULL
        [score] <> Null                 -> datatypes.score IS NOT NULL
        [score] IS NOT nULL             -> datatypes.score IS NOT NULL
        coalesce([score], 0.14)         -> coalesce(datatypes.score, 0.14)
        coalesce([department], "moo")   -> coalesce(datatypes.department, 'moo')
        coalesce([test_date], date("2020-01-01"))   -> coalesce(datatypes.test_date, '2020-01-01')
        """
        self.validate_examples(good_examples, partial(self.builder.parse))

        cast_examples = """
        string([score]) like "9_"       -> CAST(datatypes.score AS VARCHAR) LIKE '9_'
        string([score])                 -> CAST(datatypes.score AS VARCHAR)
        string(score)                   -> CAST(datatypes.score AS VARCHAR)
        int(department)                 -> CAST(datatypes.department AS INTEGER)
        int([department])               -> CAST(datatypes.department AS INTEGER)
        """
        self.validate_examples(
            cast_examples, partial(self.builder.parse), exclude_drivernames=["bigquery"]
        )

        cast_bq_examples = """
        string([score]) like "9_"       -> CAST(datatypes.score AS STRING) LIKE '9_'
        string([score])                 -> CAST(datatypes.score AS STRING)
        string(score)                   -> CAST(datatypes.score AS STRING)
        int(department)                 -> CAST(datatypes.department AS INT64)
        int([department])               -> CAST(datatypes.department AS INT64)
        """
        self.validate_examples(
            cast_bq_examples,
            partial(self.builder.parse),
            include_drivernames=["bigquery"],
        )

        sqlite_examples = """
        # Parentheses make this work
        valid_score AND [score] > 2     -> datatypes.valid_score = 1 AND datatypes.score > 2
        (valid_score = TRUE) AND score > 5 -> datatypes.valid_score = 1 AND datatypes.score > 5
        [department] like "foo"         -> datatypes.department LIKE '%foo%'
        [department] ilike "foo%"       -> lower(datatypes.department) LIKE lower('foo%')
        "F" + [department] ILIKE "f__"  -> lower('F' || datatypes.department) LIKE lower('f__')
        valid_score AND score > 2       -> datatypes.valid_score = 1 AND datatypes.score > 2
        (valid_score = TRUE) AND score > 5 -> datatypes.valid_score = 1 AND datatypes.score > 5
        department like "foo"           -> datatypes.department LIKE '%foo%'
        department ilike "foo%"         -> lower(datatypes.department) LIKE lower('foo%')
        "F" + department ILIKE "f__"    -> lower('F' || datatypes.department) LIKE lower('f__')
        """
        self.validate_examples(
            sqlite_examples, partial(self.builder.parse), include_drivernames=["sqlite"]
        )

        postgres_examples = """
        # Parentheses make this work
        valid_score AND [score] > 2     -> datatypes.valid_score AND datatypes.score > 2
        (valid_score = TRUE) AND score > 5 -> datatypes.valid_score = true AND datatypes.score > 5
        [department] like "foo"         -> datatypes.department LIKE '%%foo%%'
        [department] ilike "foo%"       -> datatypes.department ILIKE 'foo%%'
        "F" + [department] ILIKE "f__"  -> 'F' || datatypes.department ILIKE 'f__'
        valid_score AND score > 2       -> datatypes.valid_score AND datatypes.score > 2
        (valid_score = TRUE) AND score > 5 -> datatypes.valid_score = true AND datatypes.score > 5
        department like "foo"           -> datatypes.department LIKE '%%foo%%'
        department ilike "foo%"         -> datatypes.department ILIKE 'foo%%'
        "F" + department ILIKE "f__"    -> 'F' || datatypes.department ILIKE 'f__'
        """
        self.validate_examples(
            postgres_examples,
            partial(self.builder.parse),
            include_drivernames=["postgres"],
        )

    def test_division_and_math(self):
        """These examples should all succeed"""

        good_examples = """
        [score] / 2                      -> CAST(datatypes.score AS FLOAT) / 2
        [score] / 2.0                    -> CAST(datatypes.score AS FLOAT) / 2.0
        sum(score) / count(*)            -> CASE WHEN (count(*) = 0) THEN NULL ELSE CAST(sum(datatypes.score) AS FLOAT) / CAST(count(*) AS FLOAT) END
        sum([score] / 1)                 -> sum(datatypes.score)
        sum([score] / [score])           -> sum(CASE WHEN (datatypes.score = 0) THEN NULL ELSE CAST(datatypes.score AS FLOAT) / CAST(datatypes.score AS FLOAT) END)
        score / 2                        -> CAST(datatypes.score AS FLOAT) / 2
        sum(score / score)               -> sum(CASE WHEN (datatypes.score = 0) THEN NULL ELSE CAST(datatypes.score AS FLOAT) / CAST(datatypes.score AS FLOAT) END)
        [score] / (2/1)                  -> CAST(datatypes.score AS FLOAT) / 2
        [score] / (0.5/0.25)             -> CAST(datatypes.score AS FLOAT) / 2.0
        [score] / (0.5 /    0.25)        -> CAST(datatypes.score AS FLOAT) / 2.0
        [score] * (2*3)                  -> datatypes.score * 6
        [score] * (2*score)              -> datatypes.score * 2 * datatypes.score
        [score] * (2 / score)            -> datatypes.score * CASE WHEN (datatypes.score = 0) THEN NULL ELSE 2 / CAST(datatypes.score AS FLOAT) END
        [score] / (10-7)                 -> CAST(datatypes.score AS FLOAT) / 3
        ([score] + [score]) / ([score] - [score]) -> CASE WHEN (datatypes.score - datatypes.score = 0) THEN NULL ELSE CAST(datatypes.score + datatypes.score AS FLOAT) / CAST(datatypes.score - datatypes.score AS FLOAT) END
        score + (3 + 5 / (10 - 5))       -> datatypes.score + 4.0
        # Order of operations has: score + (3 + 0.5 - 5)
        score + (3 + 5 / 10 - 5)         -> datatypes.score + -1.5
        """
        self.validate_examples(
            good_examples,
            partial(self.builder.parse),
            exclude_drivernames=["bigquery"],
        )

        good_examples = good_examples.replace("AS FLOAT", "AS FLOAT64")
        self.validate_examples(
            good_examples, partial(self.builder.parse), include_drivernames=["bigquery"]
        )

        div_1_examples = """
        [score]                          -> datatypes.score
        [ScORE]                          -> datatypes.score
        [score] / 1                      -> datatypes.score
        [score] / (10-9)                 -> datatypes.score
        """
        self.validate_examples(
            div_1_examples,
            partial(self.builder.parse),
            exclude_drivernames=["bigquery"],
        )

        div_1_bq_examples = """
        [score]                          -> score
        [ScORE]                          -> score
        [score] / 1                      -> score
        [score] / (10-9)                 -> score
        """
        self.validate_examples(
            div_1_bq_examples,
            partial(self.builder.parse),
            include_drivernames=["bigquery"],
        )

    def test_no_brackets(self):
        """Brackets are optional around field names"""

        good_examples = """
        ScORE + ScORE                 -> datatypes.score + datatypes.score
        score + 2.0                   -> datatypes.score + 2.0
        username + department         -> datatypes.username || datatypes.department
        "foo" + department            -> 'foo' || datatypes.department
        1.0 + score                   -> 1.0 + datatypes.score
        1.0 + score + score           -> 1.0 + datatypes.score + datatypes.score
        -0.1 * score + 600            -> -0.1 * datatypes.score + 600
        -0.1 * score + 600.0          -> -0.1 * datatypes.score + 600.0
        score = score                 -> datatypes.score = datatypes.score
        score >= 2.0                  -> datatypes.score >= 2.0
        2.0 <= score                  -> datatypes.score >= 2.0
        NOT score >= 2.0              -> datatypes.score < 2.0
        NOT 2.0 <= score              -> datatypes.score < 2.0
        score > 3 AND true            -> datatypes.score > 3
        score = Null                  -> datatypes.score IS NULL
        score IS NULL                 -> datatypes.score IS NULL
        score != Null                 -> datatypes.score IS NOT NULL
        score <> Null                 -> datatypes.score IS NOT NULL
        score IS NOT nULL             -> datatypes.score IS NOT NULL
        coalesce(score, 0.14)         -> coalesce(datatypes.score, 0.14)
        coalesce(department, "moo")   -> coalesce(datatypes.department, 'moo')
        """
        self.validate_examples(good_examples, partial(self.builder.parse))

    def test_arrays(self):
        good_examples = """
        [score] NOT in (1,2,3)            -> (datatypes.score NOT IN (1, 2, 3))
        [score] In (1,2,   3.0)           -> datatypes.score IN (1, 2, 3)
        [score] In (1)                    -> datatypes.score IN (1)
        NOT [score] In (1)                -> (datatypes.score NOT IN (1))
        NOT NOT [score] In (1)            -> datatypes.score IN (1)
        [department] In ("A", "B")        -> datatypes.department IN ('A', 'B')
        [department] In ("A", "B",)       -> datatypes.department IN ('A', 'B')
        [department] iN  (  "A",    "B" ) -> datatypes.department IN ('A', 'B')
        [department] In ("A",)            -> datatypes.department IN ('A')
        [department] In ("A")             -> datatypes.department IN ('A')
        [department] + [username] In ("A", "B")        -> datatypes.department || datatypes.username IN ('A', 'B')
        """
        self.validate_examples(good_examples, partial(self.builder.parse))

    def test_boolean(self):
        good_examples = """
        [score] > 3                                           -> datatypes.score > 3
        [department] > "b"                                    -> datatypes.department > 'b'
        [score] > 3 AND [score] < 5                           -> datatypes.score > 3 AND datatypes.score < 5
        [score] > 3 AND [score] < 5 AND [score] = 4           -> datatypes.score > 3 AND datatypes.score < 5 AND datatypes.score = 4
        [score] > 3 AND True                                  -> datatypes.score > 3
        NOT [score] > 3 AND [score] < 5                       -> NOT (datatypes.score > 3 AND datatypes.score < 5)
        NOT ([score] > 3 AND [score] < 5)                     -> NOT (datatypes.score > 3 AND datatypes.score < 5)
        (NOT [score] > 3) AND [score] < 5                     -> datatypes.score <= 3 AND datatypes.score < 5
        # The following is a unexpected result but not sure how to fix it
        NOT [score] > 3 AND NOT [score] < 5                   ->  NOT (datatypes.score > 3 AND datatypes.score >= 5)
        [score] > 3 OR [score] < 5                            -> datatypes.score > 3 OR datatypes.score < 5
        [score] > 3 AND [score] < 5 OR [score] = 4            -> datatypes.score > 3 AND datatypes.score < 5 OR datatypes.score = 4
        [score] > 3 AND ([score] < 5 OR [score] = 4)          -> datatypes.score > 3 AND (datatypes.score < 5 OR datatypes.score = 4)
        [score] > 3 AND [score] < 5 OR [score] = 4 AND [score] = 3 -> datatypes.score > 3 AND datatypes.score < 5 OR datatypes.score = 4 AND datatypes.score = 3
        [score] > 3 AND ([score] < 5 OR [score] = 4) AND [score] = 3 -> datatypes.score > 3 AND (datatypes.score < 5 OR datatypes.score = 4) AND datatypes.score = 3
        [score] between 1 and 3                               -> datatypes.score BETWEEN 1 AND 3
        [score] between [score] and [score]                   -> datatypes.score BETWEEN datatypes.score AND datatypes.score
        [username] between "a" and "z"                        -> datatypes.username BETWEEN 'a' AND 'z'
        [username] between [department] and "z"               -> datatypes.username BETWEEN datatypes.department AND 'z'
        count_distinct([score] > 80)                          -> count(DISTINCT (datatypes.score > 80))
        count([score] > 80)                                   -> count(datatypes.score > 80)
        """
        self.validate_examples(good_examples, partial(self.builder.parse))

        sqlite_examples = """
        string([score]) like "9_"                             -> CAST(datatypes.score AS VARCHAR) LIKE '9_'
        [score] > 3 AND False                                 -> 0 = 1
        """
        self.validate_examples(
            sqlite_examples, partial(self.builder.parse), include_drivernames=["sqlite"]
        )

        print("lets do it\n" * 10)
        postgres_examples = """
        [score] > 3 AND False                               -> false
        """
        self.validate_examples(
            postgres_examples,
            partial(self.builder.parse),
            exclude_drivernames=["sqlite"],
        )

    def test_failure(self):
        """These examples should all fail"""

        bad_examples = """
unknown ->
unknown is not a valid column name

unknown
^
===
[scores] ->
scores is not a valid column name

[scores]
 ^
===
[scores] + -1.0 ->
scores is not a valid column name

[scores] + -1.0
 ^
unknown_col and num can not be added together

[scores] + -1.0
 ^
===
2.0 + [scores] ->
scores is not a valid column name

2.0 + [scores]
       ^
num and unknown_col can not be added together

2.0 + [scores]
^
===
[foo_b] ->
foo_b is not a valid column name

[foo_b]
 ^
===
[username] + [score] ->
string and num can not be added together

[username] + [score]
 ^
===
[username]-[score] ->
string and num can not be subtracted

[username]-[score]
 ^
===
[username] * [score] ->
string and num can not be multiplied together

[username] * [score]
 ^
===
[score] * [username] ->
num and string can not be multiplied together

[score] * [username]
 ^
===
[score]   + [department] ->
num and string can not be added together

[score]   + [department]
 ^
===
[score] = [department] ->
Can't compare num to str

[score] = [department]
 ^
===
[score] = "5" ->
Can't compare num to str

[score] = "5"
 ^
===
[department] = 3.24 ->
Can't compare str to num

[department] = 3.24
 ^
===
[department] In ("A", 2) ->
An array may not contain both strings and numbers

[department] In ("A", 2)
                 ^
===
[username] NOT IN (2, "B") ->
An array may not contain both strings and numbers

[username] NOT IN (2, "B")
                   ^
===
1 in (1,2,3) ->
Must be a column or expression

1 in (1,2,3)
^
===
NOT [department] ->
NOT requires a boolean value

NOT [department]
^
===
[score] / 0 ->
When dividing, the denominator can not be zero
===
[score] / (10-10) ->
When dividing, the denominator can not be zero
===
avg(department) ->
A str can not be aggregated using avg.

avg(department)
^
===
avg(test_date) ->
A date can not be aggregated using avg.

avg(test_date)
^
"""
        self.validate_bad_examples(bad_examples, partial(self.builder.parse))


class TestDataTypesTablePostgres(TestDataTypesTable):
    connection_string = "postgresql+psycopg2://postgres:postgres@db:5432/postgres"


class TestDataTypesTableBigquery(TestDataTypesTable):
    connection_string = get_bigquery_connection_string()
    engine_kwargs = get_bigquery_engine_kwargs()


class TestDataTypesTableDates(GrammarTestCase):
    @freeze_time("2020-01-14 09:21:34", tz_offset=utc_offset)
    def test_dates(self):
        good_examples = f"""
        [test_date] > date("2020-01-01")     -> datatypes.test_date > '2020-01-01'
        [test_date] > date("today")          -> datatypes.test_date > '2020-01-14'
        date("today") < [test_date]          -> datatypes.test_date > '2020-01-14'
        [test_date] > date("1 day ago")      -> datatypes.test_date > '2020-01-13'
        [test_date] > date("1 day")          -> datatypes.test_date > '2020-01-13'
        [test_date] > date("1 days ago")     -> datatypes.test_date > '2020-01-13'
        [test_date] between date("2020-01-01") and date("2020-01-30")      -> datatypes.test_date BETWEEN '2020-01-01' AND '2020-01-30'
        [test_date] IS last year              -> datatypes.test_date BETWEEN '2019-01-01' AND '2019-12-31'
        [test_datetime] > date("1 days ago")  -> datatypes.test_datetime > '2020-01-13 09:21:34'
        [test_datetime] between date("2020-01-01") and date("2020-01-30")      -> datatypes.test_datetime BETWEEN '2020-01-01 00:00:00' AND '2020-01-30 23:59:59.999999'
        [test_datetime] IS last year          -> datatypes.test_datetime BETWEEN '2019-01-01 00:00:00' AND '2019-12-31 23:59:59.999999'
        [test_datetime] IS next year          -> datatypes.test_datetime BETWEEN '2021-01-01 00:00:00' AND '2021-12-31 23:59:59.999999'
        # The date() wrapper function is optional
        [test_date] > "1 days ago"            -> datatypes.test_date > '2020-01-13'
        [test_datetime] > "1 days ago"        -> datatypes.test_datetime > '2020-01-13 09:21:34'
        [test_date] between "30 days ago" and "now" -> datatypes.test_date BETWEEN '2019-12-15' AND '2020-01-14'
        [test_date] between date("30 days ago") and date("now") -> datatypes.test_date BETWEEN '2019-12-15' AND '2020-01-14'
        [test_datetime] between date("30 days ago") and date("now") -> datatypes.test_datetime BETWEEN '2019-12-15 09:21:34' AND '2020-01-14 09:21:34'
        """

        self.validate_examples(good_examples, partial(self.builder.parse))

    def test_dates_without_freetime(self):
        # Can't tests with date conversions and freeze time :/
        good_examples = f"""
        month([test_date]) > date("2020-12-30")          -> date_trunc('month', datatypes.test_date) > '2020-12-30'
        month([test_datetime]) > date("2020-12-30")      -> date_trunc('month', datatypes.test_datetime) > '2020-12-30'
        date("2020-12-30") < month([test_datetime])      -> date_trunc('month', datatypes.test_datetime) > '2020-12-30'
        day([test_date]) > date("2020-12-30")            -> date_trunc('day', datatypes.test_date) > '2020-12-30'
        week([test_date]) > date("2020-12-30")           -> date_trunc('week', datatypes.test_date) > '2020-12-30'
        quarter([test_date]) > date("2020-12-30")        -> date_trunc('quarter', datatypes.test_date) > '2020-12-30'
        year([test_date]) > date("2020-12-30")           -> date_trunc('year', datatypes.test_date) > '2020-12-30'
        date([test_datetime])                            -> date_trunc('day', datatypes.test_datetime)
        date(2020, 1, 1)                                 -> date(2020, 1, 1)
        month(date(2020, 1, 1))                          -> date_trunc('month', date(2020, 1, 1))
        """
        self.validate_examples(
            good_examples, partial(self.builder.parse), exclude_drivernames=["bigquery"]
        )

        # Can't tests with date conversions and freeze time :/
        bigquery_examples = f"""
        month([test_date]) > date("2020-12-30")          -> date_trunc(datatypes.test_date, month) > '2020-12-30'
        month([test_datetime]) > date("2020-12-30")      -> datetime(timestamp_trunc(datatypes.test_datetime, month)) > '2020-12-30'
        date("2020-12-30") < month([test_datetime])      -> datetime(timestamp_trunc(datatypes.test_datetime, month)) > '2020-12-30'
        day([test_date]) > date("2020-12-30")            -> date_trunc(datatypes.test_date, day) > '2020-12-30'
        week([test_date]) > date("2020-12-30")           -> date_trunc(datatypes.test_date, week(monday)) > '2020-12-30'
        quarter([test_date]) > date("2020-12-30")        -> date_trunc(datatypes.test_date, quarter) > '2020-12-30'
        year([test_date]) > date("2020-12-30")           -> date_trunc(datatypes.test_date, year) > '2020-12-30'
        date([test_datetime])                            -> datetime(timestamp_trunc(datatypes.test_datetime, day))
        date(2020, 1, 1)                                 -> date(2020, 1, 1)
        month(date(2020, 1, 1))                          -> date_trunc(date(2020, 1, 1), month)
        """
        self.validate_examples(
            bigquery_examples,
            partial(self.builder.parse),
            include_drivernames=["bigquery"],
        )

    def test_failure(self):
        """These examples should all fail"""

        bad_examples = """
[test_date] > date("1 day from now") ->

Can't convert '1 day from now' to a date.
===
[test_date] between date("2020-01-01") and 7 ->
When using between, the column (date) and between values (date, num) must be the same data type.

[test_date] between date("2020-01-01") and 7
 ^
===
[test_date] between "potato" and date("2020-01-01") ->
Can't convert 'potato' to a date.
"""
        self.validate_bad_examples(bad_examples, partial(self.builder.parse))


class TestDataTypesTableDatesPostgres(TestDataTypesTableDates):
    connection_string = "postgresql+psycopg2://postgres:postgres@db:5432/postgres"


class TestDataTypesTableDatesBigquery(TestDataTypesTableDates):
    connection_string = get_bigquery_connection_string()
    engine_kwargs = get_bigquery_engine_kwargs()


class TestAggregations(GrammarTestCase):
    def test_allow_aggregation(self):
        # Can't tests with date conversions and freeze time :/
        good_examples = f"""
        count_distinct(department = "MO" AND score > 20) -> count(DISTINCT (datatypes.department = 'MO' AND datatypes.score > 20))
        count_distinct(if(department = "MO" AND score > 20, department)) -> count(DISTINCT CASE WHEN (datatypes.department = 'MO' AND datatypes.score > 20) THEN datatypes.department END)
        count(IF(department = "MO" AND score > 20, department)) -> count(CASE WHEN (datatypes.department = 'MO' AND datatypes.score > 20) THEN datatypes.department END)
        count(*)                     -> count(*)
        sum([score])                 -> sum(datatypes.score)
        sum(score)                   -> sum(datatypes.score)
        sum(score*2.0)               -> sum(datatypes.score * 2.0)
        avg(score)                   -> avg(datatypes.score)
        min(test_date)               -> min(datatypes.test_date)
        max(test_datetime)           -> max(datatypes.test_datetime)
        max(score) - min(score)      -> max(datatypes.score) - min(datatypes.score)
        count_distinct([score])      -> count(DISTINCT datatypes.score)
        count_distinct([department]) -> count(DISTINCT datatypes.department)
        count_distinct(department)   -> count(DISTINCT datatypes.department)
        """
        self.validate_examples(
            good_examples, partial(self.builder.parse, forbid_aggregation=False)
        )

    def test_forbid_aggregation(self):
        """These examples should all fail"""

        bad_examples = """
sum([score]) ->
Aggregations are not allowed in this field.

sum([score])
^
===
sum(score) ->
Aggregations are not allowed in this field.

sum(score)
^
===
sum(department) ->
A str can not be aggregated using sum.

sum(department)
^
===
2.1235 + sum(department) ->
A str can not be aggregated using sum.

2.1235 + sum(department)
         ^
===
sum(score) + sum(department) ->
Aggregations are not allowed in this field.

sum(score) + sum(department)
^
A str can not be aggregated using sum.

sum(score) + sum(department)
             ^
===
sum(score) + sum(department) ->
Aggregations are not allowed in this field.

sum(score) + sum(department)
^
A str can not be aggregated using sum.

sum(score) + sum(department)
             ^
"""
        self.validate_bad_examples(
            bad_examples, partial(self.builder.parse, forbid_aggregation=True)
        )

    def test_bad_aggregations(self):
        """These examples should all fail"""

        bad_examples = """
sum(department) ->
A str can not be aggregated using sum.

sum(department)
^
===
2.1235 + sum(department) ->
A str can not be aggregated using sum.

2.1235 + sum(department)
         ^
===
sum(score) + sum(department) ->
A str can not be aggregated using sum.

sum(score) + sum(department)
             ^
"""

        self.validate_bad_examples(bad_examples, partial(self.builder.parse))

        sqlite_examples = """
percentile1([score]) ->
Percentile is not supported on sqlite

percentile1([score])
^
===
percentile13([score]) ->
Percentile values of 13 are not supported.

percentile13([score])
^
Percentile is not supported on sqlite

percentile13([score])
^
        """
        self.validate_bad_examples(
            sqlite_examples, partial(self.builder.parse), include_drivernames=["sqlite"]
        )

        postgres_examples = """
percentile13([score]) ->
Percentile values of 13 are not supported.

percentile13([score])
^
        """
        self.validate_bad_examples(
            postgres_examples,
            partial(self.builder.parse),
            include_drivernames=["postgres"],
        )

    def test_percentiles(self):
        # TODO: build these tests
        # Can't test with sqlalchemy
        postgres_percentiles = f"""
        percentile1([score])                  -> percentile_cont(0.01) WITHIN GROUP (ORDER BY datatypes.score)
        percentile50([score])                 -> percentile_cont(0.5) WITHIN GROUP (ORDER BY datatypes.score)
        percentile90([score])                 -> percentile_cont(0.9) WITHIN GROUP (ORDER BY datatypes.score)
        """

        self.validate_examples(
            postgres_percentiles,
            partial(self.builder.parse, forbid_aggregation=False),
            include_drivernames=["postgres"],
        )

        bigquery_percentiles = f"""
        percentile1([score])                  -> approx_quantiles(datatypes.score, 100)[OFFSET(1)]
        percentile50([score])                 -> approx_quantiles(datatypes.score, 2)[OFFSET(1)]
        percentile90([score])                 -> approx_quantiles(datatypes.score, 10)[OFFSET(9)]
        """

        self.validate_examples(
            bigquery_percentiles,
            partial(self.builder.parse, forbid_aggregation=False),
            include_drivernames=["bigquery"],
        )


class TestAggregationsPostgres(TestAggregations):
    connection_string = "postgresql+psycopg2://postgres:postgres@db:5432/postgres"


class TestAggregationsBigquery(TestAggregations):
    connection_string = get_bigquery_connection_string()
    engine_kwargs = get_bigquery_engine_kwargs()


class TestIf(GrammarTestCase):
    def test_if(self):
        good_examples = f"""
        # Number if statements
        if([valid_score], [score], -1)                                  -> CASE WHEN datatypes.valid_score THEN datatypes.score ELSE -1 END
        if([score] > 2, [score], -1)                                    -> CASE WHEN (datatypes.score > 2) THEN datatypes.score ELSE -1 END
        if([score] > 2, [score])                                        -> CASE WHEN (datatypes.score > 2) THEN datatypes.score END
        if([score] > 2, [score]) + if([score] > 4, 1)                   -> CASE WHEN (datatypes.score > 2) THEN datatypes.score END + CASE WHEN (datatypes.score > 4) THEN 1 END
        if([score] > 2, [score] + if([score] > 4, 1))                   -> CASE WHEN (datatypes.score > 2) THEN datatypes.score + CASE WHEN (datatypes.score > 4) THEN 1 END END
        if([score] > 2, [score], [score] > 4, [score]*2.0, -5)          -> CASE WHEN (datatypes.score > 2) THEN datatypes.score WHEN (datatypes.score > 4) THEN datatypes.score * 2.0 ELSE -5 END
        if([score] > 2, null, [score] > 4, [score]*2.0, -5)             -> CASE WHEN (datatypes.score > 2) THEN NULL WHEN (datatypes.score > 4) THEN datatypes.score * 2.0 ELSE -5 END
        if([score] > 2, null, [score] > 4, [score]*2.0, NULL)           -> CASE WHEN (datatypes.score > 2) THEN NULL WHEN (datatypes.score > 4) THEN datatypes.score * 2.0 END
        if([score] > 2, [SCORE]/2.24, [score] > 4, [score]*2.0, [score] > 6.0, [score]*3.5, NULL)           -> CASE WHEN (datatypes.score > 2) THEN CAST(datatypes.score AS FLOAT) / 2.24 WHEN (datatypes.score > 4) THEN datatypes.score * 2.0 WHEN (datatypes.score > 6.0) THEN datatypes.score * 3.5 END
        if([score] > 2 OR score = 1, [score]*3.5)                       -> CASE WHEN (datatypes.score > 2 OR datatypes.score = 1) THEN datatypes.score * 3.5 END
        # String if statements
        if(department = "Radiology", "XDR-Radiology")                   -> CASE WHEN (datatypes.department = 'Radiology') THEN 'XDR-Radiology' END
        if([score] > 2, "XDR-Radiology")                                -> CASE WHEN (datatypes.score > 2) THEN 'XDR-Radiology' END
        if([score] > 2, "XDR-Radiology", "OTHERS")                      -> CASE WHEN (datatypes.score > 2) THEN 'XDR-Radiology' ELSE 'OTHERS' END
        if([score] > 2, "XDR-Radiology", "OTHERS"+department)           -> CASE WHEN (datatypes.score > 2) THEN 'XDR-Radiology' ELSE 'OTHERS' || datatypes.department END
        if([score] > 2, "XDR-Radiology", "OTHERS") + department         -> CASE WHEN (datatypes.score > 2) THEN 'XDR-Radiology' ELSE 'OTHERS' END || datatypes.department
        # This is actually an error, but we allow it for now
        if([score] > 2, NULL, "OTHERS") + department                    -> CASE WHEN (datatypes.score > 2) THEN NULL ELSE 'OTHERS' END || datatypes.department
        if([score] > 2, department, score > 4, username, "OTHERS")      -> CASE WHEN (datatypes.score > 2) THEN datatypes.department WHEN (datatypes.score > 4) THEN datatypes.username ELSE 'OTHERS' END
        # Date if statements
        if([score] > 2, test_date)                                      -> CASE WHEN (datatypes.score > 2) THEN datatypes.test_date END
        if(test_date > date("2020-01-01"), test_date)                   -> CASE WHEN (datatypes.test_date > '2020-01-01') THEN datatypes.test_date END
        # Datetime if statements
        if(test_datetime > date("2020-01-01"), test_datetime)           -> CASE WHEN (datatypes.test_datetime > '2020-01-01 00:00:00') THEN datatypes.test_datetime END
        if(score<2,"babies",score<13,"children",score<20,"teens","oldsters")       -> CASE WHEN (datatypes.score < 2) THEN 'babies' WHEN (datatypes.score < 13) THEN 'children' WHEN (datatypes.score < 20) THEN 'teens' ELSE 'oldsters' END
        if((score)<2,"babies",(score)<13,"children",(score)<20,"teens","oldsters") -> CASE WHEN (datatypes.score < 2) THEN 'babies' WHEN (datatypes.score < 13) THEN 'children' WHEN (datatypes.score < 20) THEN 'teens' ELSE 'oldsters' END
        if(department = "1", score, department="2", score*2)            -> CASE WHEN (datatypes.department = '1') THEN datatypes.score WHEN (datatypes.department = '2') THEN datatypes.score * 2 END
        """

        self.validate_examples(
            good_examples, partial(self.builder.parse), exclude_drivernames=["bigquery"]
        )
        bq_examples = good_examples.replace("AS FLOAT", "AS FLOAT64")
        self.validate_examples(
            bq_examples, partial(self.builder.parse), include_drivernames=["bigquery"]
        )

        if_examples = """
        month(if([score] > 2, test_date))                               -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_date END)
        month(if([score] > 2, test_datetime))                           -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END)
        """
        self.validate_examples(
            if_examples, partial(self.builder.parse), exclude_drivernames=["bigquery"]
        )
        bq_if_examples = """
        month(if([score] > 2, test_date))                               -> date_trunc(CASE WHEN (datatypes.score > 2) THEN datatypes.test_date END, month)
        month(if([score] > 2, test_datetime))                           -> datetime(timestamp_trunc(CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END, month))
        """
        self.validate_examples(
            bq_if_examples,
            partial(self.builder.parse),
            include_drivernames=["bigquery"],
        )

        sqlite_examples = """
if([score] > 2, test_datetime)                                  -> CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END
"""
        self.validate_examples(
            sqlite_examples, partial(self.builder.parse), include_drivernames=["sqlite"]
        )
        postgres_examples = """
if([score] > 2, test_datetime)                                -> CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END
"""
        self.validate_examples(
            postgres_examples,
            partial(self.builder.parse),
            include_drivernames=["postgres"],
        )

    def test_failing_if(self):
        """These examples should all fail"""

        bad_examples = """
if(department, score) ->
This should be a boolean column or expression

if(department, score)
   ^
===
if(department = 2, score) ->
Can't compare str to num

if(department = 2, score)
   ^
===
if(department = "1", score, department, score*2) ->
This should be a boolean column or expression

if(department = "1", score, department, score*2)
                            ^
===
if(department = "1", score, valid_score, score*2, department, 12.5) ->
This should be a boolean column or expression

if(department = "1", score, valid_score, score*2, department, 12.5)
                                                  ^
===
if(department, score, valid_score, score*2) ->
This should be a boolean column or expression

if(department, score, valid_score, score*2)
   ^
===
if(department = "foo", score, valid_score, department) ->
The values in this if statement must be the same type, not num and str

if(department = "foo", score, valid_score, department)
                                           ^
===
if(department = "foo", department, valid_score, score) ->
The values in this if statement must be the same type, not str and num

if(department = "foo", department, valid_score, score)
                                                ^
"""
        self.validate_bad_examples(
            bad_examples, partial(self.builder.parse, forbid_aggregation=True)
        )


class TestIfPostgres(TestIf):
    connection_string = "postgresql+psycopg2://postgres:postgres@db:5432/postgres"


class TestIfBigquery(TestIf):
    connection_string = get_bigquery_connection_string()
    engine_kwargs = get_bigquery_engine_kwargs()


class TestSQLAlchemySerialize(GrammarTestCase):
    """Test we can serialize and deserialize parsed results using
    sqlalchemy.ext.serialize. This is important because parsing is
    costly."""

    def test_ser_deser(self):
        # Can't tests with date conversions and freeze time :/
        good_examples = f"""
        sum([score])                 -> sum(datatypes.score)
        sum(score)                   -> sum(datatypes.score)
        month(if([score] > 2, test_datetime))                           -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END)
        if(test_datetime > date("2020-01-01"), test_datetime)           -> CASE WHEN (datatypes.test_datetime > '2020-01-01 00:00:00') THEN datatypes.test_datetime END
        month(if([score] > 2, test_datetime))                           -> date_trunc('month', CASE WHEN (datatypes.score > 2) THEN datatypes.test_datetime END)
        if(score<2,"babies",score<13,"children",score<20,"teens","oldsters")       -> CASE WHEN (datatypes.score < 2) THEN 'babies' WHEN (datatypes.score < 13) THEN 'children' WHEN (datatypes.score < 20) THEN 'teens' ELSE 'oldsters' END
        if((score)<2,"babies",(score)<13,"children",(score)<20,"teens","oldsters") -> CASE WHEN (datatypes.score < 2) THEN 'babies' WHEN (datatypes.score < 13) THEN 'children' WHEN (datatypes.score < 20) THEN 'teens' ELSE 'oldsters' END
        """

        for field, expected_sql in self.examples(good_examples):
            expr, _ = self.builder.parse(field, forbid_aggregation=False, debug=True)
            ser = dumps(expr)
            expr = loads(ser, self.builder.selectable.metadata, self.dbinfo.Session())
            self.assertEqual(expr_to_str(expr, engine=self.dbinfo.engine), expected_sql)


class TestIsValidColumn(GrammarTestCase):
    def test_is_valid_column(self):
        good_values = [
            "this",
            "that",
            "THIS",
            "THAT",
            "this_that_and_other",
            "_other",
            "THIS_that_",
            "that ",
            "TH AT  ",
            "x",
            "_",
            "5",
            "_5",
        ]
        for v in good_values:
            self.assertTrue(is_valid_column(v))

        bad_values = [
            "for_slackbot}_organization_name",
            " this",
            " THIS",
            "this_that-and-other",
        ]
        for v in bad_values:
            self.assertFalse(is_valid_column(v))
