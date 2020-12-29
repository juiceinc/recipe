from tests.test_base import Scores2
from recipe.schemas.lark_grammar import Builder
from unittest import TestCase

def to_sql(expr):
    """Utility to print sql for a expression """
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


class TestScores2(TestCase):
    def test_success(self):
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
        -0.1 * [score] + 600            -> -0.1 * scores.score + 600.0
        [score] = [score]               -> scores.score = scores.score
        [score] >= 2.0                  -> scores.score >= 2.0
        2.0 <= [score]                  -> scores.score >= 2.0
        """

        b = Builder(Scores2)

        for row in good_examples.split("\n"):
            if row.strip():
                row, expected = row.split("->")
                expected = expected.strip()

                print(f"\nInput: {row}")
                expr = b.parse(row)
                self.assertEqual(to_sql(expr), expected)

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

        for row in bad_examples.split("\n"):
            if row.strip():
                row, expected_msg = row.split("->")
                print(row)
                expected_msg = expected_msg.strip()
                print(expected_msg)
                with self.assertRaises(Exception) as e:                    
                    expr = b.parse(row)
                self.assertEqual(str(e.exception), expected_msg)
                print("except:", e.exception)

