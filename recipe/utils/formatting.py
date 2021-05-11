from uuid import uuid4

import sqlparse
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import UnsupportedCompilationError
from sqlalchemy.orm import Query
from sqlalchemy.sql.sqltypes import Date, DateTime, NullType, String


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
    if isinstance(statement, Query):
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
