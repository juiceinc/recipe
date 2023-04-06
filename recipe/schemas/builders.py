from datetime import date, datetime

import structlog
from lark import GrammarError, Lark
from sqlalchemy import (
    func,
    text,
    cast,
    String,
    Date,
    DateTime,
    Integer,
    Float,
    Boolean,
    alias,
)
from typing import List, Optional

from .expression_grammar import (
    make_grammar,
    make_column_collection_for_selectable,
    make_column_collection_for_constant_literals,
    make_column_collection_for_constant_expressions,
    has_constant_expressions,
    has_constant_literals,
)
from .transformers import TransformToSQLAlchemyExpression
from .utils import mkkey
from .validators import SQLALchemyValidator

SLOG = structlog.get_logger(__name__)


LARK_CACHE = {}


class SQLAlchemyBuilder:
    @classmethod
    def get_builder(
        cls,
        selectable,
        *,
        constants: Optional[dict] = None,
        extra_selectables: Optional[List] = None,
        cache=None,
    ):
        return cls(
            selectable,
            constants=constants,
            extra_selectables=extra_selectables,
            cache=cache,
        )

    @classmethod
    def clear_builder_cache(cls):
        LARK_CACHE.clear()

    def __init__(
        self,
        selectable,
        *,
        constants: Optional[dict] = None,
        extra_selectables: Optional[List] = None,
        cache=None,
    ):
        """Parse a recipe field by building a custom grammar that
        uses the colums in a selectable.

        Args:
            selectable (Table): A SQLAlchemy selectable

        Keyword Args:
            extra_selectables (list): A list containing pairs of selectable,
              namespace string.
            cache (cache): An optional cache.
        """
        self.selectable = selectable
        # Database driver
        try:
            self.drivername = selectable.metadata.bind.url.drivername
        except Exception:
            self.drivername = "unknown"

        self.cache = cache

        constants = constants or {}
        constant_expressions_cc = None

        # If we have expressions, we'll build a select statement
        # using the expressions, and make these into constants.
        # Note, constants can only be aggregate expressions on the base selectable.
        if has_constant_expressions(constants):
            self.columns = make_column_collection_for_selectable(selectable)
            self.finalize_grammar()

            constant_expressions_cc = make_column_collection_for_constant_expressions(
                self, constants, namespace="constants"
            )

        self.columns = make_column_collection_for_selectable(selectable)

        if constant_expressions_cc:
            self.columns.extend(constant_expressions_cc)

        # Add literal constants
        if has_constant_literals(constants):
            self.columns.extend(
                make_column_collection_for_constant_literals(
                    constants=constants, namespace="constants"
                )
            )

        if extra_selectables:
            for selectable, namespace in extra_selectables:
                self.columns.extend(
                    make_column_collection_for_selectable(
                        selectable, namespace=namespace
                    )
                )

        self.finalize_grammar()

    def finalize_grammar(self):
        """Once we have a set of columns, we can generate the parser and transformer"""
        self.grammar = make_grammar(self.columns)
        grammar_hash = mkkey("grammar", self.grammar)
        # Developer Note: cache key
        # This cache key is used for both LARK_CACHE as well as the SQLAlchemy
        # expressions that we use in `parse` below. This key must change any time the
        # table columns change, which it *should* do because the grammar contains
        # information about all types and columns in the table. If that ever changes
        # (e.g. we switch to separate steps for parsing and type/variable validation),
        # we'll need to calculate the expression hash key separately.
        self.cache_key = f"recipe-expr:{grammar_hash}"
        try:
            self.cached_trees = (
                self.cache.get(self.cache_key, {}) if self.cache is not None else None
            )
        except Exception:
            SLOG.exception("ingredient-cache-error")
            self.cached_trees = None
        if self.cache_key in LARK_CACHE:
            self.parser = LARK_CACHE[self.cache_key]
        else:
            # Constructing this Lark parser can take a significant amount of time (like,
            # nearly 1 second for some large tables), which is why we cache parsers in
            # LARK_CACHE. Unfortunately, by using an in-process cache, we are still
            # computing this redundantly quite often, because Juicebox processes get
            # cycled often and there are many workers in a given deployment.
            #
            # Lark supports serializing parsers to speed up loading, but unfortunately
            # it only supports this for LALR parsers, and we are using Earley.
            self.parser = Lark(
                self.grammar,
                parser="earley",
                ambiguity="resolve",
                start="col",
                propagate_positions=True,
                # predict_all=True,
            )
            LARK_CACHE[self.cache_key] = self.parser

        self.transformer = TransformToSQLAlchemyExpression(
            self.selectable, self.columns, self.drivername
        )

        # The data type of the last parsed expression
        self.last_datatype = None

    def parse(
        self,
        text,
        forbid_aggregation=False,
        enforce_aggregation=False,
        debug=False,
        convert_dates_with=None,
        convert_datetimes_with=None,
    ):
        """Return a parse tree for text

        Args:
            text (str): A field expression
            forbid_aggregation (bool, optional):
              The expression may not contain aggregations. Defaults to False.
            enforce_aggregation (bool, optional):
              Wrap the expression in an aggregation if one is not provided. Defaults to False.
            debug (bool, optional): Show some debug info. Defaults to False.
            convert_dates_with (str, optional): A converter to use for date fields
            convert_datetimes_with (str, optional): A converter to use for datetime fields

        Raises:
            GrammarError: A description of any errors and where they occur

        Returns:
            A tuple of
                ColumnElement: A SQLALchemy expression
                DataType: The datatype of the expression (bool, date, datetime, num, str)
        """
        key = mkkey(
            "parsed-ingredient",
            text,
            forbid_aggregation,
            enforce_aggregation,
            convert_dates_with,
            convert_datetimes_with,
        )
        cache_result = (
            self.cached_trees.get(key) if self.cached_trees is not None else None
        )

        extra_args = (
            key,
            enforce_aggregation,
            debug,
            convert_dates_with,
            convert_datetimes_with,
        )

        if cache_result is None:
            (tree, validator) = self._parse(text, forbid_aggregation)
            return self.tree_to_expression(tree, validator, *extra_args)
        else:
            (tree, validator) = cache_result
            try:
                return self.tree_to_expression(tree, validator, *extra_args)
            except Exception:
                SLOG.exception("cached-tree-to-validator-error")
                # If we get ANY error while dealing with the cached ingredient data, we
                # should just retry everything without using the cache. There could be
                # any number of things wrong with the cache, e.g. if it was produced on
                # an older version of Recipe (there are a lot of internal implementation
                # details encoded into the cached data).
                del self.cached_trees[key]
                (tree, validator) = self._parse(text, forbid_aggregation)
                return self.tree_to_expression(tree, validator, *extra_args)

    def _parse(self, text, forbid_aggregation):
        tree = self.parser.parse(text, start="col")
        validator = SQLALchemyValidator(text, forbid_aggregation, self.drivername)
        validator.visit(tree)
        return (tree, validator)

    def tree_to_expression(
        self,
        tree,
        validator,
        key,
        enforce_aggregation,
        debug,
        convert_dates_with,
        convert_datetimes_with,
    ):
        self.last_datatype = validator.last_datatype
        if validator.errors:
            if debug:
                print("".join(validator.errors))
                print("Tree:\n" + tree.pretty())
            raise GrammarError("".join(validator.errors))

        if debug:
            print("Tree:\n" + tree.pretty())
        self.transformer.text = text
        self.transformer.convert_dates_with = convert_dates_with
        self.transformer.convert_datetimes_with = convert_datetimes_with
        expr = self.transformer.transform(tree)

        # Wrap literal expressions in a cast so they can be labeled
        if isinstance(expr, (str, float, int, date, datetime, bool)):
            if isinstance(expr, str):
                self.last_datatype = "str"
                expr = cast(expr, String)
            elif isinstance(expr, date):
                self.last_datatype = "date"
                expr = cast(expr, Date)
            elif isinstance(expr, datetime):
                self.last_datatype = "datetime"
                expr = cast(expr, DateTime)
            elif isinstance(expr, int):
                self.last_datatype = "num"
                expr = cast(expr, Integer)
            elif isinstance(expr, float):
                self.last_datatype = "num"
                expr = cast(expr, Float)
            elif isinstance(expr, bool):
                self.last_datatype = "bool"
                expr = cast(expr, Boolean)
            else:
                raise GrammarError("Unsure of the datatype of {expr}")

        if (
            enforce_aggregation
            and not validator.found_aggregation
            and self.last_datatype == "num"
        ):
            result = (func.sum(expr), self.last_datatype)
        else:
            result = (expr, self.last_datatype)

        if self.cached_trees is not None:
            self.cached_trees[key] = (tree, validator)
        return result

    def save_cache(self):
        # see "Developer Note: cache key" for info about cache keys.
        try:
            self.cache.set(self.cache_key, self.cached_trees)
        except Exception:
            SLOG.exception("shelf-save-cache-error")
