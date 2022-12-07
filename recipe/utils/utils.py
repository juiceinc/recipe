import math
import re
import unicodedata
from recipe.exceptions import IngredientNotFoundError

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


def calculate_dynamic_ingredient(selections:dict, ingr:str) -> str:
    """Convert an ingredient string from a dynamic value to a literal

    If selections is {"selection": "student"}

    @selection
                    Return `student`
    @selection2|default:"course"
                    Return `course` (because selection2 was not found)
    @selection|prefix:id_|default:"course"
                    Return `id_student`
    """
    if not ingr.startswith("@"):
        return ingr
    ingredient_parts = ingr[1:].split("|")
    ingr_name = ingredient_parts[0]
    ingr_name = selections.get(ingr_name)
    if isinstance(ingr_name, (list, tuple)):
        ingr_name = ingr_name[0]
    directives = {
        "default": lambda ingr, default: ingr or default,
        "prefix": lambda ingr, prefix: ingr if ingr is None else prefix + ingr,
        "suffix": lambda ingr, suffix: ingr if ingr is None else ingr + suffix,
    }
    for directive in ingredient_parts[1:]:
        directive_parts = directive.split(":")
        directive_name = directive_parts[0]
        if directive_name not in directives:
            raise ValueError(f"Unknown ingredient directive: {directive_parts}")
        ingr_name = directives[directive_name](ingr_name, *directive_parts[1:])

    if not ingr_name:
        raise IngredientNotFoundError(f"Couldn't find an ingredient based on: {ingr}\nAvailable selections: {selections.keys()}", ingr)

    return ingr_name
