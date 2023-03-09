import attr
import structlog
from lark import Tree, Visitor
from lark.lexer import Token

SLOG = structlog.get_logger(__name__)


@attr.s
class SQLALchemyValidator(Visitor):
    """
    Visit the tree and return descriptive information. Populate
    a list of errors.

    Args:
        text (str): A copy of the parsed text for error descriptions
        forbid_aggregation (bool): Should aggregations be treated as an error
        drivername (str): The database engine we are running against
    """

    text = attr.ib()
    forbid_aggregation = attr.ib()
    drivername = attr.ib()
    # Was an aggregation encountered in the tree?
    found_aggregation = attr.ib(default=False)
    # What is the datatype of the returned expression
    last_datatype = attr.ib(default=None)
    # Errors encountered while visiting the tree
    errors = attr.ib(factory=list)

    def _data_type(self, tree):
        # Find the data type for a tree
        if tree is None:
            return None
        if tree.data == "col":
            dt = self._data_type(tree.children[0])
        else:
            dt = tree.data
        dt = str(dt)  # Convert Tokens to strings
        if dt == "datetime_end":
            dt = "datetime"
        elif dt == "string":
            dt = "str"
        elif dt == "boolean":
            dt = "bool"
        return dt

    def _add_error(self, message, tree):
        """Add an error pointing to this location in the parsed string"""
        tok = None
        # Find the first token
        while tree and tree.children:
            tree = tree.children[0]
            if isinstance(tree, Token):
                tok = tree
                break

        if tok:
            extra_context = self._get_context_for_token(tok, span=200)
            message = f"{message}\n\n{extra_context}"
        self.errors.append(message)

    def _get_context_for_token(self, tok, span=40):
        pos = tok.start_pos
        start = max(pos - span, 0)
        end = pos + span
        before = self.text[start:pos].rsplit("\n", 1)[-1]
        after = self.text[pos:end].split("\n", 1)[0]
        return before + after + "\n" + " " * len(before) + "^\n"

    def _error_math(self, tree, verb):
        tok1 = tree.children[0].children[0]
        tok2 = tree.children[1].children[0]
        self._add_error(f"{tok1.data} and {tok2.data} can not be {verb}", tree)

    def col(self, tree):
        self.last_datatype = self._data_type(tree)

    def error_add(self, tree):
        self._error_math(tree, "added together")

    def error_mul(self, tree):
        self._error_math(tree, "multiplied together")

    def error_sub(self, tree):
        self._error_math(tree, "subtracted")

    def error_div(self, tree):
        self._error_math(tree, "divided")

    def error_if_statement(self, tree):
        args = tree.children
        # Throw away the "if"
        args = args[1:]

        # If there's an odd number of args, pop the last one to use as the else
        if len(args) % 2:
            else_expr = args.pop()
        else:
            else_expr = None

        # The "odd" args should be booleans
        bool_args = args[::2]
        # The "even" args should be values of the same type
        value_args = args[1::2]

        # Check that the boolean args are boolean
        for arg in bool_args:
            dt = self._data_type(arg)
            if dt != "bool":
                self._add_error("This should be a boolean column or expression", arg)

        # Data types of columns must match
        value_type = None
        for arg in value_args + [else_expr]:
            dt = self._data_type(arg)
            if dt is not None:
                if value_type is None:
                    value_type = dt
                elif value_type != dt:
                    self._add_error(
                        f"The values in this if statement must be the same type, not {value_type} and {dt}",
                        arg,
                    )

    def aggr(self, tree):
        self.found_aggregation = True
        if self.forbid_aggregation:
            self._add_error("Aggregations are not allowed in this field.", tree)

    def unknown_col(self, tree):
        """Column name doesn't exist in the data"""
        tok1 = tree.children[0]
        self._add_error(f"{tok1} is not a valid column name", tree)

    def unusable_col(self, tree):
        """Column name isn't a data type we can handle"""
        tok1 = tree.children[0]
        self._add_error(
            f"{tok1} is a data type that can't be used. Usable data types are strings, numbers, boolean, dates, and datetimes",
            tree,
        )

    def error_not_nonboolean(self, tree):
        """NOT string or NOT num"""
        self._add_error("NOT requires a boolean value", tree)

    def mixedarray(self, tree):
        """An array containing a mix of strings and numbers"""
        self._add_error("An array may not contain both strings and numbers", tree)

    def vector_expr(self, tree):
        val, comp, arr = tree.children
        # If the left hand side is a number or string primitive
        if isinstance(val.children[0], Token) and val.children[0].type in (
            "NUMBER",
            "ESCAPED_STRING",
        ):
            self._add_error("Must be a column or expression", val)

    def error_aggr(self, tree):
        """Aggregating a bad data type"""
        fn = tree.children[0].children[0]
        dt = self._data_type(tree.children[0].children[1])
        self._add_error(f"A {dt} can not be aggregated using {fn}.", tree)

    def error_between_expr(self, tree):
        col, BETWEEN, left, AND, right = tree.children
        col_type = self._data_type(col)
        left_type = self._data_type(left)
        right_type = self._data_type(right)
        if col_type == "datetime":
            if left_type == "date":
                left_type = "datetime"
            if right_type == "date":
                right_type = "datetime"
        if not (col_type == left_type == right_type):
            self._add_error(
                f"When using between, the column ({col_type}) and between values ({left_type}, {right_type}) must be the same data type.",
                tree,
            )

    def bool_expr(self, tree):
        """a > b where the types of a and b don't match"""
        left, _, right = tree.children
        if isinstance(left, Tree) and isinstance(right, Tree):
            left_data_type = self._data_type(left)
            right_data_type = self._data_type(right)
            if left_data_type == right_data_type == "date":
                return
            if left_data_type == right_data_type == "datetime":
                return
            if left_data_type in ("date", "datetime") and right_data_type == "string":
                # Strings will be auto converted
                return
            if left_data_type != right_data_type:
                self._add_error(
                    f"Can't compare {left_data_type} to {right_data_type}", tree
                )

    def percentile_aggr(self, tree):
        """Sum up the things"""
        percentile, fld = tree.children
        percentile_val = int(percentile[len("percentile") :])
        if percentile_val not in (1, 5, 10, 25, 50, 75, 90, 95, 99):
            self._add_error(
                f"Percentile values of {percentile_val} are not supported.", tree
            )
        if self.drivername == "sqlite":
            self._add_error("Percentile is not supported on sqlite", tree)
