import math
import re
import unicodedata

from sqlalchemy.sql.functions import FunctionElement

WHITESPACE_RE = re.compile(r"\s+", flags=re.DOTALL | re.MULTILINE)


def replace_whitespace_with_space(s):
    """Replace multiple whitespaces with a single space."""
    return WHITESPACE_RE.sub(" ", s)


def clean_unicode(value):
    """Convert value into ASCII bytes by brute force."""
    if not isinstance(value, str):
        value = str(value)
    try:
        return value.encode("ascii")
    except UnicodeEncodeError:
        value = unicodedata.normalize("NFKD", value)
        return value.encode("ascii", "ignore")


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def disaggregate(expr):
    if isinstance(expr, FunctionElement):
        return expr.clause_expr
    else:
        return expr


def pad_values(values, prefix="RECIPE-DUMMY-VAL-", bin_size=11):
    """Redshift recompiles queries when a where IN clause includes a
    different number of values. To avoid this, pad out the list
    of string values to certain fixed lengths.

    The default bin size is 11. During testing we discovered that
    compilation was required for where clauses with different numbers
    of items but all queries with 11 or more items shared a compiled
    query.
    """
    assert isinstance(values, (list, tuple))
    cnt = len(values)
    if cnt and isinstance(values[0], str):
        # Round up to the nearest bin_size
        desired_cnt = int(math.ceil(float(cnt) / bin_size) * bin_size)
        added_values = [prefix + str(i + 1) for i in range(desired_cnt - cnt)]
        if isinstance(values, tuple):
            return values + tuple(added_values)
        else:
            return values + added_values
    else:
        return values
