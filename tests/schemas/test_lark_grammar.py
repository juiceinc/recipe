from tests.test_base import Scores2
from recipe.schemas.lark_grammar import Builder
from unittest import TestCase, skip


def to_sql(expr):
    """Utility to print sql for a expression """
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


class TestScores2(TestCase):
    def examples(self, input_rows):
        for row in input_rows.split("\n"):
            row = row.strip()
            if row == "" or row.startswith("#"):
                continue

            field, expected = row.split("->")
            expected = expected.strip()
            yield field, expected

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
        """

        b = Builder(Scores2)

        for field, expected in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)
            self.assertEqual(to_sql(expr), expected)

    # @skip
    def test_arrays(self):
        good_examples = """
        [score] NOT in (1,2,3)            -> scores.score NOT IN (1, 2, 3)
        [score] In (1,2,   3.0)           -> scores.score IN (1, 2, 3.0)
        [score] In (1)                    -> scores.score IN (1)
        [department] In ("A", "B")        -> scores.department IN ('A', 'B')
        """

        b = Builder(Scores2)

        for field, expected in self.examples(good_examples):
            print(f"\nInput: {field}")
            expr = b.parse(field, debug=True)
            self.assertEqual(to_sql(expr), expected)

    # @skip
    def test_failure(self):
        """These examples should all fail"""

        bad_examples = """
        [scores] + -1.0             -> scores is not a valid column name, unknown_col and num can not be added together
        2.0 + [scores]              -> scores is not a valid column name, num and unknown_col can not be added together
        [foo_b]                     -> foo_b is not a valid column name
        [username] + [score]        -> string and num can not be added together
        [score]   + [department]    -> num and string can not be added together
        [score] = [department]      -> Can't compare num to string
        """
        b = Builder(Scores2)

        for field, expected in self.examples(bad_examples):
            with self.assertRaises(Exception) as e:
                b.parse(field)
            self.assertEqual(str(e.exception), expected)
            print("except:", e.exception)
