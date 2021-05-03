import importlib
import math
import re
import string
import unicodedata
from functools import wraps
from inspect import isclass, getfullargspec
from uuid import uuid4

import attr
import sqlalchemy.orm
import sqlparse
import hashlib

from faker import Faker
from faker.providers import BaseProvider
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.sqltypes import Date, DateTime, NullType, String
from sqlalchemy.exc import UnsupportedCompilationError

# only expose the printing sql function
__all__ = [
    "prettyprintable_sql",
    "generate_faker_seed",
    "clean_unicode",
    "FakerAnonymizer",
    "FakerFormatter",
    "recipe_arg",
]


def filter_to_string(filt):
    """Compile a filter object to a literal string"""
    try:
        if hasattr(filt, "filters") and filt.filters:
            return str(filt.filters[0].compile(compile_kwargs={"literal_binds": True}))
        elif hasattr(filt, "havings") and filt.havings:
            return str(filt.havings[0].compile(compile_kwargs={"literal_binds": True}))
        elif isinstance(filt, bool):
            return str(filt)
        else:
            return str(filt.compile(compile_kwargs={"literal_binds": True}))
    except UnsupportedCompilationError:
        return uuid4()


def generate_faker_seed(value):
    """Generate a seed value for faker."""
    if not isinstance(value, str):
        value = str(value)

    h = hashlib.new("md5")
    h.update(value.encode("utf-8"))
    return int(h.hexdigest()[:16], 16)


def recipe_arg(*args):
    """Decorator for recipe builder arguments.

    Promotes builder pattern by returning self.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *_args, **_kwargs):
            from recipe import Recipe, RecipeExtension, BadRecipe

            if isinstance(self, Recipe):
                recipe = self
            elif isinstance(self, RecipeExtension):
                recipe = self.recipe
            else:
                raise BadRecipe(
                    "recipe_arg can only be applied to"
                    "methods of Recipe or RecipeExtension"
                )

            if recipe._query is not None:
                recipe.reset()

            func(self, *_args, **_kwargs)
            return recipe

        return wrapper

    return decorator


class TestProvider(BaseProvider):
    """A demo faker provider for testing string providers"""

    def foo(self):
        return "foo"


class StringLiteral(String):
    """Teach SA how to literalize various things."""

    def literal_processor(self, dialect):
        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, int):
                return str(value)
            if not isinstance(value, str):
                value = str(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)
            return result

        return process


def prettyprintable_sql(statement, dialect=None, reindent=True):
    """
    Generate an SQL expression string with bound parameters rendered inline
    for the given SQLAlchemy statement. The function can also receive a
    `sqlalchemy.orm.Query` object instead of statement.

    WARNING: Should only be used for debugging. Inlining parameters is not
             safe when handling user created data.
    """
    if isinstance(statement, sqlalchemy.orm.Query):
        if dialect is None:
            dialect = statement.session.get_bind().dialect
        statement = statement.statement

    # Generate a class that can handle encoding
    if dialect:
        DialectKlass = dialect.__class__
    else:
        DialectKlass = DefaultDialect

    class LiteralDialect(DialectKlass):
        colspecs = {
            # prevent various encoding explosions
            String: StringLiteral,
            # teach SA about how to literalize a datetime
            DateTime: StringLiteral,
            Date: StringLiteral,
            # don't format py2 long integers to NULL
            NullType: StringLiteral,
        }

    compiled = statement.compile(
        dialect=LiteralDialect(), compile_kwargs={"literal_binds": True}
    )
    return sqlparse.format(str(compiled), reindent=reindent)


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


class FakerFormatter(string.Formatter):
    """A formatter that can get values from Faker generators."""

    def format_field(self, obj, format_spec):
        """

        :param obj: A faker Faker
        :param format_spec: a generator
        :return: A string generated by
        """
        generator = format_spec
        kwargs = {}
        if "|" in format_spec:
            try:
                newgenerator, potential_kwargs = format_spec.split("|")
                for part in potential_kwargs.split(","):
                    k, v = part.split("=")
                    if v == "None":
                        v = None
                    elif v == "True":
                        v = True
                    elif v == "False":
                        v = False
                    elif v.isdigit():
                        v = int(v)
                    kwargs[k] = v
                generator = newgenerator
            except ValueError:
                # If more than one "|"  don't try to parse
                # If the kwargs aren't of form x=y then don't try to parse
                pass

        value = None
        if callable(getattr(obj, generator)):
            c = getattr(obj, generator)
            argspec = getfullargspec(c)
            if len(argspec.args) == 1:
                value = getattr(obj, generator)()
            elif kwargs:
                value = getattr(obj, generator)(**kwargs)
            else:
                value = c

        if value is not None and not isinstance(value, str):
            value = str(value)
        return value or "Unknown fake generator"


@attr.s
class FakerAnonymizer(object):
    """Returns a deterministically generated fake value that depends on the
    input value."""

    format_str = attr.ib()
    postprocessor = attr.ib()
    locale = attr.ib(default="en_US")
    postprocessor = attr.ib(default=None)
    providers = attr.ib(default=None)

    def __attrs_post_init__(self):
        self.fake = Faker(self.locale)
        self.formatter = FakerFormatter()
        for p in self._clean_providers(self.providers):
            self.fake.add_provider(p)

    def _clean_providers(self, providers):
        """Convert a list of anonymizer providers into classes suitable for
        adding with faker.add_provider"""
        if not providers:
            return []

        if not isinstance(providers, (list, tuple)):
            providers = [providers]

        cleaned_providers = []
        for provider in providers:
            if isinstance(provider, str):
                # dynamically import the provider
                parts = provider.split(".")
                if len(parts) > 1:
                    _module = ".".join(parts[:-1])
                    _provider_class = parts[-1]
                    try:
                        _mod = importlib.import_module(_module)
                        _provider = getattr(_mod, _provider_class, None)
                        if _provider is None:
                            # TODO: log an issue, provider not found in module
                            continue
                        elif not issubclass(_provider, BaseProvider):
                            # TODO: log an issue, provider not generator
                            continue
                        else:
                            cleaned_providers.append(_provider)

                    except ImportError:
                        # TODO: log an issue, can't import module
                        continue
            elif isclass(provider) and issubclass(provider, BaseProvider):
                cleaned_providers.append(provider)
            else:
                # TODO: log an issue, provider is not an importable string
                #  or a ProviderBase
                continue

        return cleaned_providers

    def __call__(self, value):
        self.fake.seed_instance(generate_faker_seed(value))
        value = self.formatter.format(self.format_str, fake=self.fake)
        if self.postprocessor is None:
            return value
        else:
            return self.postprocessor(value)
