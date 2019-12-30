"""Convert parsed trees into SQLAlchemy objects """
from lark import Lark, Transformer, v_args
from sqlalchemy import func, distinct, case, and_, or_, not_
from .utils import aggregations


@v_args(inline=True)  # Affects the signatures of the methods
class CalculateField(Transformer):
    """Converts a field to a SQLAlchemy expression """

    from operator import add, sub, mul, neg

    number = float

    def __init__(self, selectable, require_aggregation=False):
        self.selectable = selectable
        self.require_aggregation = require_aggregation

    def true(self, value):
        return True

    def false(self, value):
        return False

    def null(self, value):
        return None

    def IN(self, value):
        return "IN"

    def NOTIN(self, value):
        return "NOTIN"

    def BETWEEN(self, value):
        return "BETWEEN"

    def aggregate(self, name):
        return aggregations.get(name.lower())

    def div(self, num, denom):
        """SQL safe division"""
        return case([(denom == 0, None)], else_=num / denom)

    def column(self, name):
        return getattr(self.selectable.c, name)

    def agex(self, aggr, val):
        if val == "*":
            return func.count("*")
        else:
            return aggr(val)

    def expr(self, expr):
        return expr

    def case(self, *args):
        args = list(args)

        # If there's an odd number of args, pop the last one to use as the else
        if len(args) % 2:
            else_expr = args.pop()
        else:
            else_expr = None

        # collect the other args into pairs
        # ['a','b','c','d'] --> [('a',b'), ('c','d')]
        pairs = zip(args[::2], args[1::2])
        return case(pairs, else_=else_expr)

    def relation_expr(self, left, rel, right):
        comparators = {
            "=": "__eq__",
            ">": "__gt__",
            ">=": "__gte__",
            "!=": "__ne__",
            "<": "__lt__",
            "<=": "__lte__",
        }
        if right is None:
            return
        return getattr(left, comparators[rel])(right)

    def array(self, *args):
        # TODO  check these are all the same type
        # And match the type of the column!
        return args

    def vector_relation_expr(self, left, rel, right):
        comparators = {"IN": "in_", "NOTIN": "notin_"}
        if rel == "BETWEEN":
            return left.between(*right)
        else:
            return getattr(left, comparators[rel])(right)

    def bool_expr(self, *exprs):
        if len(exprs) > 1:
            return or_(*exprs)
        else:
            return exprs[0]

    def bool_term(self, *exprs):
        if len(exprs) > 1:
            return and_(*exprs)
        else:
            return exprs[0]

    def not_bool_factor(self, expr):
        return not_(expr)
