""" Test how intelligent date ranges are constructed """

import pytest
from datetime import date
from recipe.schemas.utils import calc_date_range


class TestIntelligentDateRanges(object):
    def test_calc_date_range(self):
        data = [
            # Year + all offset dates
            [
                ("this", "year", date(2020, 12, 31)),
                (date(2020, 1, 1), date(2020, 12, 31)),
            ],
            [
                ("current", "year", date(2020, 12, 31)),
                (date(2020, 1, 1), date(2020, 12, 31)),
            ],
            [
                ("prior", "year", date(2020, 12, 31)),
                (date(2019, 1, 1), date(2019, 12, 31)),
            ],
            [
                ("previous", "year", date(2020, 12, 31)),
                (date(2019, 1, 1), date(2019, 12, 31)),
            ],
            [
                ("last", "year", date(2020, 12, 31)),
                (date(2019, 1, 1), date(2019, 12, 31)),
            ],
            [
                ("next", "year", date(2020, 12, 31)),
                (date(2021, 1, 1), date(2021, 12, 31)),
            ],
            [
                ("this", "year", date(2020, 6, 8)),
                (date(2020, 1, 1), date(2020, 12, 31)),
            ],
            [
                ("current", "year", date(2020, 6, 8)),
                (date(2020, 1, 1), date(2020, 12, 31)),
            ],
            [
                ("prior", "year", date(2020, 6, 8)),
                (date(2019, 1, 1), date(2019, 12, 31)),
            ],
            [
                ("next", "year", date(2020, 6, 8)),
                (date(2021, 1, 1), date(2021, 12, 31)),
            ],
            # Ytd
            [
                ("this", "ytd", date(2020, 12, 31)),
                (date(2020, 1, 1), date(2020, 12, 31)),
            ],
            [
                ("current", "ytd", date(2020, 12, 31)),
                (date(2020, 1, 1), date(2020, 12, 31)),
            ],
            [
                ("prior", "ytd", date(2020, 12, 31)),
                (date(2019, 1, 1), date(2019, 12, 31)),
            ],
            [
                ("next", "ytd", date(2020, 12, 31)),
                (date(2021, 1, 1), date(2021, 12, 31)),
            ],
            [("this", "ytd", date(2020, 6, 8)), (date(2020, 1, 1), date(2020, 6, 8))],
            [
                ("current", "ytd", date(2020, 6, 8)),
                (date(2020, 1, 1), date(2020, 6, 8)),
            ],
            [("prior", "ytd", date(2020, 6, 8)), (date(2019, 1, 1), date(2019, 6, 8))],
            [("next", "ytd", date(2020, 6, 8)), (date(2021, 1, 1), date(2021, 6, 8))],
            [("this", "ytd", date(2020, 1, 1)), (date(2020, 1, 1), date(2020, 1, 1))],
            # qtr
            [
                ("this", "qtr", date(2020, 12, 31)),
                (date(2020, 10, 1), date(2020, 12, 31)),
            ],
            [
                ("this", "qtr", date(2020, 10, 1)),
                (date(2020, 10, 1), date(2020, 12, 31)),
            ],
            [("this", "qtr", date(2020, 9, 30)), (date(2020, 7, 1), date(2020, 9, 30))],
            [("this", "qtr", date(2020, 6, 8)), (date(2020, 4, 1), date(2020, 6, 30))],
            [("this", "qtr", date(2020, 6, 30)), (date(2020, 4, 1), date(2020, 6, 30))],
            [("this", "qtr", date(2020, 5, 30)), (date(2020, 4, 1), date(2020, 6, 30))],
            [("this", "qtr", date(2020, 4, 1)), (date(2020, 4, 1), date(2020, 6, 30))],
            [("this", "qtr", date(2020, 3, 31)), (date(2020, 1, 1), date(2020, 3, 31))],
            [("this", "qtr", date(2020, 1, 31)), (date(2020, 1, 1), date(2020, 3, 31))],
            [("this", "qtr", date(2020, 1, 1)), (date(2020, 1, 1), date(2020, 3, 31))],
            [("next", "qtr", date(2020, 1, 1)), (date(2020, 4, 1), date(2020, 6, 30))],
            [
                ("previous", "qtr", date(2020, 1, 1)),
                (date(2019, 10, 1), date(2019, 12, 31)),
            ],
            [
                ("prior", "qtr", date(2020, 3, 31)),
                (date(2019, 10, 1), date(2019, 12, 31)),
            ],
            [("next", "qtr", date(2020, 3, 31)), (date(2020, 4, 1), date(2020, 6, 30))],
            # A leap day
            [("next", "qtr", date(2020, 2, 29)), (date(2020, 4, 1), date(2020, 6, 30))],
            # month
            [
                ("this", "month", date(2020, 12, 31)),
                (date(2020, 12, 1), date(2020, 12, 31)),
            ],
            [
                ("this", "month", date(2020, 10, 31)),
                (date(2020, 10, 1), date(2020, 10, 31)),
            ],
            # Leap and non leap days
            [
                ("this", "month", date(2020, 2, 2)),
                (date(2020, 2, 1), date(2020, 2, 29)),
            ],
            [
                ("this", "month", date(2019, 2, 2)),
                (date(2019, 2, 1), date(2019, 2, 28)),
            ],
            [
                ("next", "month", date(2019, 2, 2)),
                (date(2019, 3, 1), date(2019, 3, 31)),
            ],
            [
                ("prior", "month", date(2019, 2, 2)),
                (date(2019, 1, 1), date(2019, 1, 31)),
            ],
            # mtd
            [
                ("this", "mtd", date(2020, 12, 31)),
                (date(2020, 12, 1), date(2020, 12, 31)),
            ],
            [
                ("current", "mtd", date(2020, 12, 31)),
                (date(2020, 12, 1), date(2020, 12, 31)),
            ],
            [
                ("prior", "mtd", date(2020, 12, 31)),
                (date(2020, 11, 1), date(2020, 11, 30)),
            ],
            [
                ("next", "mtd", date(2020, 12, 31)),
                (date(2021, 1, 1), date(2021, 1, 31)),
            ],
            [("this", "mtd", date(2020, 6, 8)), (date(2020, 6, 1), date(2020, 6, 8))],
            [
                ("current", "mtd", date(2020, 6, 8)),
                (date(2020, 6, 1), date(2020, 6, 8)),
            ],
            [("prior", "mtd", date(2020, 6, 8)), (date(2020, 5, 1), date(2020, 5, 8))],
            [("next", "mtd", date(2020, 6, 8)), (date(2020, 7, 1), date(2020, 7, 8))],
            [("this", "mtd", date(2020, 1, 1)), (date(2020, 1, 1), date(2020, 1, 1))],
            # Long to short months
            [
                ("prior", "mtd", date(2020, 3, 30)),
                (date(2020, 2, 1), date(2020, 2, 29)),
            ],
            # Short to long months count the number of days that have occurred
            [("next", "mtd", date(2020, 6, 30)), (date(2020, 7, 1), date(2020, 7, 30))],
            # day
            [
                ("this", "day", date(2020, 12, 31)),
                (date(2020, 12, 31), date(2020, 12, 31)),
            ],
            [("next", "day", date(2020, 12, 31)), (date(2021, 1, 1), date(2021, 1, 1))],
            [
                ("prior", "day", date(2020, 12, 31)),
                (date(2020, 12, 30), date(2020, 12, 30)),
            ],
        ]

        for date_range_args, expected in data:
            actual = calc_date_range(*date_range_args)
            assert actual == expected

    def test_bad_inputs(self):
        with pytest.raises(ValueError):
            calc_date_range("THIS", "day", date(2020, 12, 31))

        with pytest.raises(ValueError):
            calc_date_range("flugelhorn", "day", date(2020, 12, 31))

        with pytest.raises(ValueError):
            calc_date_range("current", "domino", date(2020, 12, 31))
