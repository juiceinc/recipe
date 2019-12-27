from recipe.schemas.field_grammar import *


def test_parsers():
    """ Test expressions against parsers"""

    # An expression to parse and the parsers it should pass
    #                                                 field_parser
    #                                                 | full_condition_parser
    #                                                 | | noag_field_parser
    #                                                 | | | noag_partial_condition_parser
    #                                                 | | | | noag_any_condition_parser
    #                                                 | | | | | noag_full_condition_parser
    #                                                 v v v v v v
    values = """
    > 10                                            # 0,0,0,1,0,0
    war_total IN (1,2,3)                            # 0,1,0,0,0,1
    war_total BETWEEN (1, 5)                        # 0,1,0,0,0,1
    if(war_total BETWEEN (1, 5), 5)                 # 1,0,1,0,0,0
    if(war_total BETWEEN (1, 5), 4, 2)              # 1,0,1,0,0,0
    war_total > 10                                  # 0,1,0,0,0,1
    war_total > 10 OR NOT war_total < 20            # 0,1,0,0,0,1
    in (1,2,3)                                      # 0,0,0,1,0,0
    sum(x) < 20                                     # 0,1,0,0,0,0

    WAR_TOTAL                                       # 1,0,1,0,0,0
    war_total                                       # 1,0,1,0,0,0
    (war_total + war_total)                         # 1,0,1,0,0,0
    (war_total + war_total) / war_total             # 1,0,1,0,0,0

    couNT(*)                                        # 1,0,0,0,0,0
    AVG(war_total) + 1.0                            # 1,0,0,0,0,0
    sum(war_total)                                  # 1,0,0,0,0,0
    max(war_total) - min(war_total)                 # 1,0,0,0,0,0
    min(war_total)-max(war_total-war_total)         # 1,0,0,0,0,0
    avg(war_total + 2.0)                            # 1,0,0,0,0,0
    avg(war_total) + 2.0                            # 1,0,0,0,0,0
    min(a+b)/max(b*c)                               # 1,0,0,0,0,0
    min(2+\"a\")/max(b*c)                           # 1,0,0,0,0,0
    min(a+-2.0)/max(b*c)                            # 1,0,0,0,0,0
    min(x)                                          # 1,0,0,0,0,0
    """

    # connection = engine.connect()
    parsers = (
        ("field_parser", field_parser),
        ("full_condition_parser", full_condition_parser),
        ("noag_field_parser", noag_field_parser),
        ("noag_partial_condition_parser", noag_partial_condition_parser),
        ("noag_any_condition_parser", noag_any_condition_parser),
        ("noag_full_condition_parser", noag_full_condition_parser),
    )

    for row in values.split("\n"):
        row = row.strip()
        if row:
            row, expected = row.split('#')
            expected_by_parser = expected.strip().split(",")
            # print("\n\n\n\n{row}")
            # print('-'*40)
            for parser_name, parser in parsers:
                expected_result = expected_by_parser.pop(0)
                try:
                    tree = parser.parse(row)
                    # print(f"{parser_name:30s} succeeded ({expected_result})")
                    assert expected_result == '1'
                    print(tree.pretty())
                except:
                    # print(f"{parser_name:30s} failed ({expected_result})")
                    assert expected_result == '0'
