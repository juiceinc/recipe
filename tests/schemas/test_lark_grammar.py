from lark.parse_tree_builder import ExpandSingleChild
from tests.test_base import DataTypesTable
from recipe.schemas.lark_grammar import Builder
from freezegun import freeze_time
from unittest import TestCase, skip
import time

utc_offset = -1 * time.localtime().tm_gmtoff / 3600.0


def to_sql(expr):
    """Utility to print sql for a expression """
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


class TestBase(TestCase):
    maxDiff = None

    def examples(self, input_rows):
        """Take input where each line looks like
        field     -> expected_sql
        """
        for row in input_rows.split("\n"):
            row = row.strip()
            if row == "" or row.startswith("#"):
                continue

            field, expected_sql = row.split("->")
            expected_sql = expected_sql.strip()
            yield field, expected_sql

    def bad_examples(self, input_rows):
        """Take input where each input is separated by two newlines

        field
        expected_error

        field
        expected_error

        """
        for row in input_rows.split("\n\n"):
            lines = row.strip().split("\n")
            field = lines[0]
            expected_error = "\n".join(lines[1:]) + "\n"
            yield field, expected_error


class TestDataTypesTable(TestBase):
    maxDiff = None

    def examples(self, input_rows):
        """Take input where each line looks like
        field     -> expected_sql
        """
        for row in input_rows.split("\n"):
            row = row.strip()
            if row == "" or row.startswith("#"):
                continue

            field, expected_sql = row.split("->")
            expected_sql = expected_sql.strip()
            yield field, expected_sql

    def bad_examples(self, input_rows):
        """Take input where each input is separated by two newlines

        field
        expected_error

        field
        expected_error

        """
        for row in input_rows.split("\n\n"):
            lines = row.strip().split("\n")
            field = lines[0]
            expected_error = "\n".join(lines[1:]) + "\n"
            yield field, expected_error

    # @skip
    def test_fields_and_addition(self):
        """These examples should all succeed"""

        good_examples = """
        [score]                         -> datatypes.score
        [ScORE]                         -> datatypes.score
        [ScORE] + [ScORE]               -> datatypes.score + datatypes.score
        [score] + 2.0                   -> datatypes.score + 2.0
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
        [score] = Null                  -> datatypes.score IS NULL
        [score] IS NULL                 -> datatypes.score IS NULL
        [score] != Null                 -> datatypes.score IS NOT NULL
        [score] <> Null                 -> datatypes.score IS NOT NULL
        [score] IS NOT nULL             -> datatypes.score IS NOT NULL
        """

        b = Builder(DataTypesTable)

        for field, expected_sql in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)
            self.assertEqual(to_sql(expr), expected_sql)

    def test_no_brackets(self):
        """Brackets are optional around field names"""

        good_examples = """
        score                         -> datatypes.score
        ScORE                         -> datatypes.score
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
        """

        b = Builder(DataTypesTable)

        for field, expected_sql in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)
            self.assertEqual(to_sql(expr), expected_sql)

    # @skip
    def test_arrays(self):
        good_examples = """
        [score] NOT in (1,2,3)            -> datatypes.score NOT IN (1, 2, 3)
        [score] In (1,2,   3.0)           -> datatypes.score IN (1, 2, 3.0)
        [score] In (1)                    -> datatypes.score IN (1)
        NOT [score] In (1)                -> datatypes.score NOT IN (1)
        NOT NOT [score] In (1)            -> datatypes.score IN (1)
        [department] In ("A", "B")        -> datatypes.department IN ('A', 'B')
        [department] In ("A", "B",)       -> datatypes.department IN ('A', 'B')
        [department] iN  (  "A",    "B" ) -> datatypes.department IN ('A', 'B')
        [department] In ("A",)            -> datatypes.department IN ('A')
        [department] In ("A")             -> datatypes.department IN ('A')
        [department] + [username] In ("A", "B")        -> datatypes.department || datatypes.username IN ('A', 'B')
        """

        b = Builder(DataTypesTable)

        for field, expected_sql in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=False)
            self.assertEqual(to_sql(expr), expected_sql)

    # @skip
    def test_boolean(self):
        good_examples = """
        [score] > 3                                           -> datatypes.score > 3
        [department] > "b"                                    -> datatypes.department > 'b'
        [score] > 3 AND [score] < 5                           -> datatypes.score > 3 AND datatypes.score < 5
        [score] > 3 AND [score] < 5 AND [score] = 4           -> datatypes.score > 3 AND datatypes.score < 5 AND datatypes.score = 4
        [score] > 3 AND True                                  -> datatypes.score > 3
        [score] > 3 AND False                                 -> false
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
        """

        b = Builder(DataTypesTable)

        for field, expected_sql in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)

            if to_sql(expr) != expected_sql:
                print("===" * 10)
                print(to_sql(expr))
                print("vs")
                print(expected_sql)
                print("===" * 10)

            self.assertEqual(to_sql(expr), expected_sql)

    # @skip
    def test_failure(self):
        """These examples should all fail"""

        bad_examples = """
[scores]
scores is not a valid column name
[scores]
 ^

[scores] + -1.0
scores is not a valid column name
[scores] + -1.0
 ^
unknown_col and num can not be added together
[scores] + -1.0
 ^

2.0 + [scores]
scores is not a valid column name
2.0 + [scores]
       ^
num and unknown_col can not be added together
2.0 + [scores]
^

[foo_b]
foo_b is not a valid column name
[foo_b]
 ^

[username] + [score]
string and num can not be added together
[username] + [score]
 ^

[score]   + [department]
num and string can not be added together
[score]   + [department]
 ^

[score] = [department]
Can't compare num to string
[score] = [department]
 ^

[score] = "5"
Can't compare num to string
[score] = "5"
 ^

[department] = 3.24
Can't compare string to num
[department] = 3.24
 ^

[department] In ("A", 2)
An array may not contain both strings and numbers
[department] In ("A", 2)
                 ^

[username] NOT IN (2, "B")
An array may not contain both strings and numbers
[username] NOT IN (2, "B")
                   ^

1 in (1,2,3)
Must be a column or expression
1 in (1,2,3)
^

NOT [department]
NOT requires a boolean value
NOT [department]
^
"""

        b = Builder(DataTypesTable)

        for field, expected_error in self.bad_examples(bad_examples):
            with self.assertRaises(Exception) as e:
                b.parse(field, debug=True)
            if str(e.exception) != expected_error:
                print("===" * 10)
                print(str(e.exception))
                print("===" * 10)
            self.assertEqual(str(e.exception), expected_error)


class TestDataTypesTableDates(TestBase):
    @freeze_time("2020-01-14 09:21:34", tz_offset=utc_offset)
    def test_dates(self):
        good_examples = f"""
        [test_date]           -> datatypes.test_date
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
        """

        b = Builder(DataTypesTable)

        for field, expected_sql in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)
            self.assertEqual(to_sql(expr), expected_sql)

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
        """

        b = Builder(DataTypesTable)

        for field, expected_sql in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)
            self.assertEqual(to_sql(expr), expected_sql)

    # @skip
    def test_failure(self):
        """These examples should all fail"""

        bad_examples = """
[test_date] > date("1 day from now")
Can't convert '1 day from now' to a date.

[test_date] between date("2020-01-01") and 7
When using between, the column (date) and between values (date, num) must be the same data type.
[test_date] between date("2020-01-01") an
 ^

[test_date] between "potato" and date("2020-01-01")
When using between, the column (date) and between values (string, date) must be the same data type.
[test_date] between "potato" and date("20
 ^
"""

        b = Builder(DataTypesTable)

        for field, expected_error in self.bad_examples(bad_examples):
            with self.assertRaises(Exception) as e:
                b.parse(field, debug=True)
            if str(e.exception).strip() != expected_error.strip():
                print("===" * 10)
                print(str(e.exception))
                print("vs")
                print(expected_error)
                print("===" * 10)
            self.assertEqual(str(e.exception).strip(), expected_error.strip())
