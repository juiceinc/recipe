from lark.exceptions import LarkError

from recipe.schemas.field_grammar import *


def test_parsers():
    """ Test expressions against parsers """

    # An expression to parse and the parsers it should pass
    #                                                 field_parser
    #                                                 | noag_field_parser
    #                                                 | | full_condition_parser
    #                                                 | | | noag_full_condition_parser
    #                                                 | | | | noag_partial_condition_parser
    #                                                 | | | | | noag_any_condition_parser
    #                                                 v v v v v v
    values = """
    > 10                                            # 0,0,0,0,1,1
    in (1,2,3)                                      # 0,0,0,0,1,1
    war_total IN (1,2,3)                            # 0,0,1,1,0,1
    war_total BETWEEN 1 AND 5                       # 0,0,1,1,0,1
    war_total > 10                                  # 0,0,1,1,0,1
    war_total > 10 OR NOT war_total < 20            # 0,0,1,1,0,1
    sum(x) < 20                                     # 0,0,1,0,0,0
    WAR_TOTAL                                       # 1,1,0,0,0,0
    war_total                                       # 1,1,0,0,0,0
    (war_total + war_total)                         # 1,1,0,0,0,0
    (war_total + war_total) / war_total             # 1,1,0,0,0,0
    if(war_total BETWEEN 1 AND 5, 5)                # 1,1,0,0,0,0
    if(war_total BETWEEN 1 AND 5, 4, 2)             # 1,1,0,0,0,0
    if(war_total is null, 4, 2)                     # 1,1,0,0,0,0
    couNT(*)                                        # 1,0,0,0,0,0
    AVG(war_total) + 1.0                            # 1,0,0,0,0,0
    sum(war_total)                                  # 1,0,0,0,0,0
    sum([war_total])                                # 1,0,0,0,0,0
    max(war_total) - min(war_total)                 # 1,0,0,0,0,0
    min(war_total)-max(war_total-war_total)         # 1,0,0,0,0,0
    avg(war_total + 2.0)                            # 1,0,0,0,0,0
    count_distinct(war_total + 2.0)                 # 1,0,0,0,0,0
    avg(war_total) + 2.0                            # 1,0,0,0,0,0
    min(a+b)/max(b*c)                               # 1,0,0,0,0,0
    min(2+\"a\")/max(b*c)                           # 1,0,0,0,0,0
    min(a+-2.0)/max(b*c)                            # 1,0,0,0,0,0
    min(x)                                          # 1,0,0,0,0,0
    month(x)                                        # 1,1,0,0,0,0
    # Converters can only operate on a single column
    month(x+y)                                      # 0,0,0,0,0,0
    """

    parsers = (
        ("field_parser", field_parser),
        ("noag_field_parser", noag_field_parser),
        ("full_condition_parser", full_condition_parser),
        ("noag_full_condition_parser", noag_full_condition_parser),
        ("noag_partial_condition_parser", noag_partial_condition_parser),
        ("noag_any_condition_parser", noag_any_condition_parser),
    )

    for row in values.split("\n"):
        row = row.strip()
        if row and not row.startswith("#"):
            row, expected = row.split("#")
            expected_by_parser = expected.strip().split(",")
            # print(f"\n\n\n\n{row}")
            # print("-" * 40)
            for parser_name, parser in parsers:
                expected_result = expected_by_parser.pop(0)
                try:
                    tree = parser.parse(row)
                    # print(f"{parser_name:30s} succeeded ({expected_result})")
                    assert expected_result == "1"
                    # print(tree.pretty())
                except LarkError:
                    # print(f"{parser_name:30s} failed ({expected_result})")
                    assert expected_result == "0"


def test_complex_condition_parser():
    values = [
        (
            ">2",
            """
partial_relation_expr
  >
  number	2
""",
        ),
        (
            "A = 1 or b = 2",
            """
bool_expr
  relation_expr
    column	A
    =
    number	1
  or
  relation_expr
    column	b
    =
    number	2
""",
        ),
        (
            'A = 1 or (b = 2 AND c < "3")',
            """
bool_expr
  relation_expr
    column	A
    =
    number	1
  or
  bool_term
    relation_expr
      column	b
      =
      number	2
    AND
    relation_expr
      column	c
      <
      string_literal	"3"
""",
        ),
    ]
    for v, pretty_tree in values:
        tree = noag_any_condition_parser.parse(v)
        assert tree.pretty().strip() == pretty_tree.strip()


def test_complex_field_parser():
    """Test the parse trees generated """
    values = [
        ("[foo]", "expr\n  column\tfoo"),
        ("foo", "expr\n  column\tfoo"),
        ("True", "expr\n  true\tTrue"),
        (
            "couNT(*)",
            """
agex
  couNT
  *
""",
        ),
        (
            "min(war_total)-max(war_total-war_total)",
            """
expr
  sub
    agex
      aggregate	min
      column	war_total
    agex
      aggregate	max
      sub
        column	war_total
        column	war_total
""",
        ),
        (
            'if(age<2,"babies","oldsters")',
            """
expr
  case
    relation_expr
      column	age
      <
      number	2
    expr
      string_literal	"babies"
    expr
      string_literal	"oldsters"
""",
        ),
        (
            'if(age<2,"babies",age between 4 and 8,"kids","oldsters")',
            """
expr
  case
    relation_expr
      column	age
      <
      number	2
    expr
      string_literal	"babies"
    between_relation_expr
      column	age
      between
      number	4
      and
      number	8
    expr
      string_literal	"kids"
    expr
      string_literal	"oldsters"
""",
        ),
        (
            "if(age is null,1,0)",
            """
expr
  case
    relation_expr_using_is
      column\tage
      is
      null
    expr
      number\t1
    expr
      number\t0
""",
        ),
        (
            "if(birth_date is prior year,1,0)",
            """
expr
  case
    relation_expr_using_is
      column\tbirth_date
      is
      is_comparison
        prior
        year
    expr
      number\t1
    expr
      number\t0
""",
        ),
        (
            "if(not(birth_date is prior year),1,0)",
            """
expr
  case
    not_bool_factor
      not
      relation_expr_using_is
        column\tbirth_date
        is
        is_comparison
          prior
          year
    expr
      number\t1
    expr
      number\t0
""",
        ),
        (
            "if(birth_date is THIS qtr or age > 12,1,0)",
            """
expr
  case
    bool_expr
      relation_expr_using_is
        column\tbirth_date
        is
        is_comparison
          THIS
          qtr
      or
      relation_expr
        column\tage
        >
        number\t12
    expr
      number\t1
    expr
      number\t0
""",
        ),
    ]
    for v, pretty_tree in values:
        tree = field_parser.parse(v)
        assert tree.pretty().strip() == pretty_tree.strip()
