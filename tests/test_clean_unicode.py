# -*- coding: UTF-8 -*-
from recipe.utils import clean_unicode
from unittest import TestCase


class TestCleanUnicode(TestCase):
    def test_string(self):
        test_values = [
            ("cookies", b"cookies"),
            (22.04, b"22.04"),
            (None, b"None"),
            (
                "Falsches Üben von Xylophonmusik quält jeden größeren Zwerg",
                b"Falsches Uben von Xylophonmusik qualt jeden groeren Zwerg",
            ),
            ("«küßî»", b"kui"),
            # String dashes
            ("― Like this? ― Right.", b" Like this?  Right."),
            ("oòóôõöōŏǫȯőǒȍȏ", b"oooooooooooooo"),
            # two byte no chars
            ("“ЌύБЇ”", b""),
        ]
        for actual, expected in test_values:
            value = clean_unicode(actual)
            self.assertEqual(value, expected)
