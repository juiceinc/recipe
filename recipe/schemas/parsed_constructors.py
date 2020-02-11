"""Convert parsed trees into SQLAlchemy objects """
from lark import Lark, Transformer, v_args
from sqlalchemy import func, distinct, case, and_, or_, not_, cast, Float

from .field_grammar import (
    field_parser,
    noag_field_parser,
    noag_full_condition_parser,
    full_condition_parser,
)
from .utils import aggregations, find_column, ingredient_class_for_name, convert_value
from recipe.exceptions import BadIngredient


@v_args(inline=True)  # Affects the signatures of the methods
class TransformToSQLAlchemyExpression(Transformer):
    """Converts a field to a SQLAlchemy expression """

    from operator import add, sub, mul, neg

    def __init__(self, selectable, require_aggregation=False):
        self.selectable = selectable
        self.require_aggregation = require_aggregation

    def number(self, value):
        try:
            return int(value)
        except ValueError:
            return float(value)

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
        return case([(denom == 0, None)], else_=cast(num, Float) / cast(denom, Float))

    def column(self, name):
        return find_column(self.selectable, name)

    def agex(self, aggr, val):
        if val == "*":
            return func.count()
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
        # Convert the right into a type compatible with the left
        right = convert_value(left, right)
        return getattr(left, comparators[rel])(right)

    def array(self, *args):
        # TODO  check these are all the same type
        # And match the type of the column!
        return args

    def vector_relation_expr(self, left, rel, right):
        comparators = {"IN": "in_", "NOTIN": "notin_"}
        return getattr(left, comparators[rel.upper()])(right)

    def between_relation_expr(self, col, between, low, _, high):
        # TODO: check data types and convert dates.
        return col.between(convert_value(col, low), convert_value(col, high))

    def bool_expr(self, *exprs):
        if len(exprs) > 1:
            return or_(*exprs)
        else:
            return exprs[0]

    def string_literal(self, value):
        # Strip the quotes off of this string
        return value.value[1:-1]

    def bool_term(self, *exprs):
        if len(exprs) > 1:
            return and_(*exprs)
        else:
            return exprs[0]

    def not_bool_factor(self, expr):
        return not_(expr)


def create_ingredient_from_parsed(ingr_dict, selectable):
    """ Create an ingredient from config version 2 object . """
    kind = ingr_dict.pop("kind", "Metric")
    IngredientClass = ingredient_class_for_name(kind)
    if IngredientClass is None:
        raise BadIngredient("Unknown ingredient kind")

    if kind in ("Metric", "Dimension"):
        parser = field_parser if kind == "Metric" else noag_field_parser
        fld_defn = ingr_dict.pop("field", None)
        tree = parser.parse(fld_defn)
        # Create a sqlalchemy expression from 'field' and pass it as the first arg
        args = [
            TransformToSQLAlchemyExpression(selectable=selectable).transform(
                parser.parse(fld_defn)
            )
        ]

        # Convert quickselects to a kwarg with sqlalchemy expressions
        parsed_quickselects = []
        for qf in ingr_dict.pop("quickselects", []):
            parsed_quickselects.append(
                {
                    "name": qf["name"],
                    "condition": TransformToSQLAlchemyExpression(
                        selectable=selectable
                    ).transform(noag_full_condition_parser.parse(qf.get("condition"))),
                }
            )
        ingr_dict["quickselects"] = parsed_quickselects

        # Convert extra fields to sqlalchemy expressions and add them directly to
        # the kwargs
        for extra in ingr_dict.pop("extra_fields", []):
            ingr_dict[extra.get("name")] = TransformToSQLAlchemyExpression(
                selectable=selectable
            ).transform(parser.parse(extra.get("field")))

    elif kind == "Filter":
        # Create a sqlalchemy expression from 'condition' and pass it as the first arg
        args = [
            TransformToSQLAlchemyExpression(selectable=selectable).transform(
                noag_full_condition_parser.parse(ingr_dict.pop("condition", None))
            )
        ]

    elif kind == "Having":
        # Create a sqlalchemy expression from 'condition' and pass it as the first arg
        # TODO: Force this to be an aggregate
        args = [
            TransformToSQLAlchemyExpression(selectable=selectable).transform(
                full_condition_parser.parse(ingr_dict.pop("condition", None))
            )
        ]

    return IngredientClass(*args, **ingr_dict)
