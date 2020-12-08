# -*- coding: UTF-8 -*-
from recipe.utils import clean_unicode


class TestCleanUnicode(object):
    def test_string(self):
        value = clean_unicode("cookies")
        assert value, b"cookies"

    def test_nonstring(self):
        value = clean_unicode(22.04)
        assert value == b"22.04"

        value = clean_unicode(None)
        assert value == b"None"

    def test_unicode_string(self):
        test_string = u"Falsches Üben von Xylophonmusik quält jeden größeren" " Zwerg"
        expected_string = (
            b"Falsches Uben von Xylophonmusik qualt jeden " b"groeren Zwerg"
        )
        value = clean_unicode(test_string)
        assert value == expected_string

    def test_unicode_string_single_upper(self):
        test_string = u"«küßî»"
        expected_string = b"kui"
        value = clean_unicode(test_string)
        assert value == expected_string

    def test_unicode_string_dashes(self):
        test_string = u"― Like this? ― Right."
        expected_string = b" Like this?  Right."
        value = clean_unicode(test_string)
        assert value == expected_string

    def test_unicode_string_o_walk(self):
        test_string = u"oòóôõöōŏǫȯőǒȍȏ"
        expected_string = b"oooooooooooooo"
        value = clean_unicode(test_string)
        assert value == expected_string

    def test_unicode_string_two_byte_no_chars(self):
        test_string = u"“ЌύБЇ”"
        expected_string = b""
        value = clean_unicode(test_string)
        assert value == expected_string
