import re
import unicodedata

import sqlalchemy.orm
import sqlparse
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.sqltypes import Date, DateTime, NullType, String

from recipe.compat import basestring, integer_types, str

# only expose the printing sql function
__all__ = ['prettyprintable_sql', 'clean_unicode']


class StringLiteral(String):
    """ Teach SA how to literalize various things. """

    def literal_processor(self, dialect):
        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, integer_types):
                return str(value)
            if not isinstance(value, basestring):
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
        dialect=LiteralDialect(), compile_kwargs={
            'literal_binds': True
        }
    )
    return sqlparse.format(str(compiled), reindent=reindent)


WHITESPACE_RE = re.compile(r'\s+', flags=re.DOTALL | re.MULTILINE)


def replace_whitespace_with_space(s):
    """ Replace multiple whitespaces with a single space. """
    return WHITESPACE_RE.sub(' ', s)


def clean_unicode(value):
    try:
        cleaned_value = str(value)
    except UnicodeEncodeError:
        cleaned_value = unicodedata.normalize('NFKD',
                                              value).encode('ascii', 'ignore')
        if not cleaned_value:
            raise ValueError('Could not find useful chars in the string')
    return cleaned_value


class AttrDict(dict):

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def disaggregate(expr):
    if isinstance(expr, FunctionElement):
        return expr.clause_expr
    else:
        return expr
