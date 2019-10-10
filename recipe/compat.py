# -*- coding: utf-8 -*-
"""
recipe.compat
~~~~~~~~~~~~~~~

This module handles import compatibility issues between Python 2 and
Python 3.

Borrowed liberally from requests.
"""

import sys

# -------
# Pythons
# -------

# Syntax sugar.
_ver = sys.version_info

#: Python 2.x?
is_py2 = _ver[0] == 2

#: Python 3.x?
is_py3 = _ver[0] == 3

# ---------
# Specifics
# ---------

if is_py2:
    builtin_str = str
    bytes = str
    str = unicode  # noqa: F821
    basestring = basestring  # noqa: F821
    numeric_types = (int, long, float)  # noqa: F821
    integer_types = (int, long)  # noqa: F821

elif is_py3:
    builtin_str = str
    str = str
    bytes = bytes
    basestring = (str, bytes)
    numeric_types = (int, float)
    integer_types = (int,)
