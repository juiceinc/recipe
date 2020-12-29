from tests.test_base import Scores2
from recipe.schemas.lark_grammar import Builder
from unittest import TestCase, skip


def to_sql(expr):
    """Utility to print sql for a expression """
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


class TestScores2(TestCase):
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
        [score]                         -> scores.score
        [ScORE]                         -> scores.score
        [ScORE] + [ScORE]               -> scores.score + scores.score
        [score] + 2.0                   -> scores.score + 2.0
        [username] + [department]       -> scores.username || scores.department
        "foo" + [department]            -> 'foo' || scores.department
        1.0 + [score]                   -> 1.0 + scores.score
        1.0 + [score] + [score]         -> 1.0 + scores.score + scores.score
        -0.1 * [score] + 600            -> -0.1 * scores.score + 600
        -0.1 * [score] + 600.0          -> -0.1 * scores.score + 600.0
        [score] = [score]               -> scores.score = scores.score
        [score] >= 2.0                  -> scores.score >= 2.0
        2.0 <= [score]                  -> scores.score >= 2.0
        NOT [score] >= 2.0              -> scores.score < 2.0
        NOT 2.0 <= [score]              -> scores.score < 2.0
        """

        b = Builder(Scores2)

        for field, expected_sql in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)
            self.assertEqual(to_sql(expr), expected_sql)

    # @skip
    def test_arrays(self):
        good_examples = """
        [score] NOT in (1,2,3)            -> scores.score NOT IN (1, 2, 3)
        [score] In (1,2,   3.0)           -> scores.score IN (1, 2, 3.0)
        [score] In (1)                    -> scores.score IN (1)
        NOT [score] In (1)                -> scores.score NOT IN (1)
        NOT NOT [score] In (1)                -> scores.score IN (1)
        [department] In ("A", "B")        -> scores.department IN ('A', 'B')
        [department] In ("A",)            -> scores.department IN ('A')
        [department] In ("A")             -> scores.department IN ('A')
        [department] + [username] In ("A", "B")        -> scores.department || scores.username IN ('A', 'B')
        """

        b = Builder(Scores2)

        for field, expected_sql in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)
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

[department] In ("A", 2)
An array may not contain both strings and numbers
[department] In ("A", 2)
                 ^

[username] NOT IN (2, "B")
An array may not contain both strings and numbers
[username] NOT IN (2, "B")
                   ^

NOT [department]
NOT requires a boolean value
NOT [department]
^
"""

        b = Builder(Scores2)

        for field, expected_error in self.bad_examples(bad_examples):
            with self.assertRaises(Exception) as e:
                b.parse(field, debug=True)
            if str(e.exception) != expected_error:
                print("===" * 10)
                print(str(e.exception))
                print("===" * 10)
            self.assertEqual(str(e.exception), expected_error)
