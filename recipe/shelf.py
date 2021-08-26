from copy import copy

from lark.exceptions import VisitError
from collections import OrderedDict
from sqlalchemy import Float, Integer, String, Table
from sqlalchemy.util import lightweight_named_tuple
from sureberus import errors as E
from sureberus import normalize_schema
from yaml import safe_load

from recipe.exceptions import BadIngredient, BadRecipe, InvalidColumnError
from recipe.ingredients import Dimension, Filter, Ingredient, Metric, InvalidIngredient
from recipe.schemas import shelf_schema
from recipe.schemas.parsed_constructors import create_ingredient_from_parsed
from recipe.schemas.config_constructors import create_ingredient_from_config

from recipe.schemas.lark_grammar import SQLAlchemyBuilder

_POP_DEFAULT = object()


def ingredient_from_validated_dict(ingr_dict, selectable, builder=None):
    """Create an ingredient object from a validated ingredient schema"""
    try:
        version = ingr_dict.pop("_version", "1")
        if version == "1":
            return create_ingredient_from_config(ingr_dict, selectable)
        else:
            return create_ingredient_from_parsed(ingr_dict, builder)
    except InvalidColumnError as e:
        error = {"type": "invalid_column", "extra": {"column_name": e.column_name}}
        return InvalidIngredient(error=error)
    except VisitError as e:
        # Lark returns the InvalidColumnError wrapped in a VisitError
        if isinstance(e.orig_exc, InvalidColumnError):
            # custom exception handling
            error = {
                "type": "invalid_column",
                "extra": {"column_name": e.orig_exc.column_name},
            }
            return InvalidIngredient(error=error)
        else:
            raise


class Shelf(object):
    """Holds ingredients used by a recipe.

    Can be initialized with no arguments, but also accepts:
    - a dictionary of ingredients as a positional argument
    - ingredients as keyword arguments

    These keyword arguments have special meaning:

    :param select_from: The SQLALchemy-compatible object which
        will be queried (usually a Table or ORM object).
    :param table: Unused, but stored on the `Meta` attribute.
    :param metadata: Unused, but stored on the `Meta` attribute.
    """

    class Meta:
        anonymize = False
        table = None
        select_from = None
        ingredient_order = []
        metadata = None

    def __init__(self, *args, **kwargs):
        self.Meta = type(self).Meta()
        self.Meta.ingredient_order = []
        self.Meta.table = kwargs.pop("table", None)
        self.Meta.select_from = kwargs.pop("select_from", None)
        self.Meta.metadata = kwargs.pop("metadata", None)
        self._ingredients = {}
        self.update(*args, **kwargs)

    # Dict Interface

    def get(self, k, d=None):
        ingredient = self._ingredients.get(k, d)
        if isinstance(ingredient, Ingredient):
            ingredient.id = k
            ingredient.anonymize = self.Meta.anonymize
        return ingredient

    def items(self):
        """Return an iterator over the ingredient names and values."""
        return self._ingredients.items()

    def values(self):
        """Return an iterator over the ingredients."""
        return self._ingredients.values()

    def keys(self):
        """Return an iterator over the ingredient keys."""
        return self._ingredients.keys()

    def __copy__(self):
        meta = copy(self.Meta)
        ingredients = copy(self._ingredients)
        new_shelf = type(self)(ingredients)
        new_shelf.Meta = meta
        return new_shelf

    def __iter__(self):
        return iter(self._ingredients)

    def __getitem__(self, key):
        """Set the id and anonymize property of the ingredient whenever we
        get or set items"""
        ingr = self._ingredients[key]
        # Ensure the ingredient's `anonymize` matches the shelf.

        # TODO: this is nasty, but *somewhat* safe because we are (hopefully)
        # guaranteed to "own" copies of all of our ingredients. It would be
        # much better if Shelf had logic that ran when anonymize is set to
        # update all ingredients. Or better yet, the code that anonymizes
        # queries should just look at the shelf instead of the ingredients.

        # One way in this is "spooky" is:
        # ingr = shelf['foo']
        # # ingr.anonymize is now False
        # shelf.Meta.anonymize = True
        # # ingr.anonymize is still False
        # shelf['foo] # ignore result
        # # ingr.anonymize is now True

        ingr.anonymize = self.Meta.anonymize
        return ingr

    def __setitem__(self, key, ingredient):
        """Set the id and anonymize property of the ingredient whenever we
        get or set items"""
        # Maintainer's note: try to make all mutation of self._ingredients go
        # through this method, so we can reliably copy & annotate the
        # ingredients that go into the Shelf.
        if not isinstance(ingredient, Ingredient):
            raise TypeError(
                "Can only set Ingredients as items on Shelf. "
                "Got: {!r}".format(ingredient)
            )
        ingredient_copy = copy(ingredient)
        ingredient_copy.id = key
        ingredient_copy.anonymize = self.Meta.anonymize
        self._ingredients[key] = ingredient_copy

    def __contains__(self, key):
        return key in self._ingredients

    def __len__(self):
        return len(self._ingredients)

    def clear(self):
        self._ingredients.clear()

    def update(self, d=None, **kwargs):
        items = []
        if d is not None:
            items = list(d.items())
        for k, v in items + list(kwargs.items()):
            self[k] = v

    def pop(self, k, d=_POP_DEFAULT):
        """Pop an ingredient off of this shelf."""
        if d is _POP_DEFAULT:
            return self._ingredients.pop(k)
        else:
            return self._ingredients.pop(k, d)

    # End dict interface

    def ingredients(self):
        """Return the ingredients in this shelf in a deterministic order"""
        return sorted(list(self.values()))

    @property
    def dimension_ids(self):
        """Return the Dimensions on this shelf in the order in which
        they were used."""
        return self._sorted_ingredients(
            [d.id for d in self.values() if isinstance(d, Dimension)]
        )

    @property
    def metric_ids(self):
        """Return the Metrics on this shelf in the order in which
        they were used."""
        return self._sorted_ingredients(
            [d.id for d in self.values() if isinstance(d, Metric)]
        )

    @property
    def filter_ids(self):
        """Return the Filters on this shelf in the order in which
        they were used."""
        return self._sorted_ingredients(
            [d.id for d in self.values() if isinstance(d, Filter)]
        )

    def _sorted_ingredients(self, ingredients):
        def sort_key(id):
            if id in self.Meta.ingredient_order:
                return self.Meta.ingredient_order.index(id)
            else:
                return 9999

        return tuple(sorted(ingredients, key=sort_key))

    def __repr__(self):
        """A string representation of the ingredients used in a recipe
        ordered by Dimensions, Metrics, Filters, then Havings
        """
        lines = []
        # sort the ingredients by type
        for ingredient in sorted(self.values()):
            lines.append(ingredient.describe())
        return "\n".join(lines)

    def use(self, ingredient):
        if not isinstance(ingredient, Ingredient):
            raise TypeError(
                "Can only set Ingredients as items on Shelf. "
                "Got: {!r}".format(ingredient)
            )

        # Track the order in which ingredients are added.
        self.Meta.ingredient_order.append(ingredient.id)
        self[ingredient.id] = ingredient

    @classmethod
    def from_config(
        cls,
        obj,
        selectable,
        ingredient_constructor=ingredient_from_validated_dict,
        metadata=None,
    ):
        """Create a shelf using a dict shelf definition.

        :param obj: A Python dictionary describing a Shelf.
        :param selectable: A SQLAlchemy Table, a Recipe, a table name, or a
            SQLAlchemy join to select from.
        :param metadata: If `selectable` is passed as a table name, then in
            order to introspect its schema, we must have the SQLAlchemy
            MetaData object to associate it with.
        :return: A shelf that contains the ingredients defined in obj.
        """
        from recipe import Recipe

        if isinstance(selectable, Recipe):
            selectable = selectable.subquery()
        elif isinstance(selectable, str):
            if "." in selectable:
                schema, tablename = selectable.split(".")
            else:
                schema, tablename = None, selectable

            selectable = Table(
                tablename, metadata, schema=schema, extend_existing=True, autoload=True
            )

        try:
            validated_shelf = normalize_schema(shelf_schema, obj, allow_unknown=True)
        except E.SureError as e:
            raise BadIngredient(str(e))
        d = {}
        builder = None

        for k, v in validated_shelf.items():
            if ingredient_constructor == ingredient_from_validated_dict:
                version = str(v.get("_version", "1"))
                if version == "1":
                    d[k] = ingredient_constructor(v, selectable)
                else:
                    if builder is None:
                        builder = SQLAlchemyBuilder.get_builder(selectable=selectable)
                    d[k] = ingredient_constructor(v, selectable, builder=builder)
            else:
                d[k] = ingredient_constructor(v, selectable)

            if isinstance(d[k], InvalidIngredient):
                if not d[k].error.get("extra"):
                    d[k].error["extra"] = {}
                d[k].error["extra"]["ingredient_name"] = k
        shelf = cls(d, select_from=selectable)

        return shelf

    @classmethod
    def from_yaml(cls, yaml_str, selectable, **kwargs):
        """Shim that calls from_validated_yaml.

        This used to call a different implementation of yaml parsing
        """
        return cls.from_validated_yaml(yaml_str, selectable, **kwargs)

    @classmethod
    def from_validated_yaml(cls, yaml_str, selectable, **kwargs):
        """Create a shelf using a yaml shelf definition.

        :param yaml_str: A string containing yaml ingredient definitions.
        :param selectable: A SQLAlchemy Table, a Recipe, or a SQLAlchemy
            join to select from.
        :return: A shelf that contains the ingredients defined in yaml_str.
        """
        obj = safe_load(yaml_str)
        return cls.from_config(obj, selectable, **kwargs)

    def find(self, obj, filter_to_class=Ingredient, constructor=None):
        """
        Find an Ingredient, optionally using the shelf.

        :param obj: A string or Ingredient
        :param filter_to_class: The Ingredient subclass that obj must be an
         instance of
        :param constructor: An optional callable for building Ingredients
         from obj
        :return: An Ingredient of subclass `filter_to_class`
        """
        if callable(constructor):
            obj = constructor(obj, shelf=self)

        if isinstance(obj, str):
            set_descending = obj.startswith("-")
            if set_descending:
                obj = obj[1:]

            if obj not in self:
                raise BadRecipe("{} doesn't exist on the shelf".format(obj))

            ingredient = self[obj]
            if isinstance(ingredient, InvalidIngredient):
                # allow InvalidIngredient, it will be handled at a later time
                return ingredient

            if not isinstance(ingredient, filter_to_class):
                raise BadRecipe("{} is not a {}".format(obj, filter_to_class))

            if set_descending:
                ingredient.ordering = "desc"

            return ingredient
        elif isinstance(obj, filter_to_class):
            return obj
        else:
            raise BadRecipe("{} is not a {}".format(obj, filter_to_class))

    def brew_query_parts(self, order_by_keys=[]):
        """Make columns, group_bys, filters, havings"""
        columns, group_bys, filters, havings = [], [], set(), set()
        order_by_keys = list(order_by_keys)
        all_filters = set()

        for ingredient in self.ingredients():
            if ingredient.error:
                error_type = ingredient.error.get("type")
                if error_type == "invalid_column":
                    extra = ingredient.error.get("extra", {})
                    column_name = extra.get("column_name")
                    ingredient_name = extra.get("ingredient_name")
                    error_msg = 'Invalid column "{0}" in ingredient "{1}"'.format(
                        column_name, ingredient_name
                    )
                    raise InvalidColumnError(error_msg, column_name=column_name)
                raise BadIngredient(str(ingredient.error))
            if ingredient.query_columns:
                columns.extend(ingredient.query_columns)
            if ingredient.group_by:
                group_bys.extend(ingredient.group_by)
            if ingredient.filters:
                # Ensure we don't add duplicate filters
                for new_f in ingredient.filters:
                    from recipe.utils import filter_to_string

                    new_f_str = filter_to_string(new_f)
                    if new_f_str not in all_filters:
                        filters.add(new_f)
                        all_filters.add(new_f_str)
            if ingredient.havings:
                havings.update(ingredient.havings)

            # If there is an order_by key on one of the ingredients, make sure
            # the recipe orders by this ingredient
            if "order_by" in ingredient.roles:
                if (
                    ingredient.id not in order_by_keys
                    and "-" + ingredient.id not in order_by_keys
                ):
                    if ingredient.ordering == "desc":
                        order_by_keys.append("-" + ingredient.id)
                    else:
                        order_by_keys.append(ingredient.id)

        order_bys = OrderedDict()
        for key in order_by_keys:
            try:
                ingr = self.find(key, (Dimension, Metric))
                for c in ingr.order_by_columns:
                    # Avoid duplicate order by columns
                    if str(c) not in [str(o) for o in order_bys]:
                        order_bys[c] = None
            except BadRecipe as e:
                # Ignore order_by if the dimension/metric is not used.
                # TODO: Add structlog warning
                pass

        return {
            "columns": columns,
            "group_bys": group_bys,
            "filters": filters,
            "havings": havings,
            "order_bys": list(order_bys.keys()),
        }

    def enchant(self, data, cache_context=None):
        """Add any calculated values to each row of a resultset generating a
        new namedtuple

        :param data: a list of row results
        :param cache_context: optional extra context for caching
        :return: a list with ingredient.cauldron_extras added for all
                 ingredients
        """
        enchantedlist = []
        if data:
            sample_item = data[0]

            # Extra fields to add to each row
            # With extra callables
            extra_fields, extra_callables = [], []

            for ingredient in self.ingredients():
                if not isinstance(ingredient, (Dimension, Metric)):
                    continue
                if cache_context:
                    ingredient.cache_context += str(cache_context)
                for extra_field, extra_callable in ingredient.cauldron_extras:
                    extra_fields.append(extra_field)
                    extra_callables.append(extra_callable)

            # Mixin the extra fields
            keyed_tuple = lightweight_named_tuple(
                "result", sample_item._fields + tuple(extra_fields)
            )

            # Iterate over the results and build a new namedtuple for each row
            for row in data:
                values = row + tuple(fn(row) for fn in extra_callables)
                enchantedlist.append(keyed_tuple(values))

        return enchantedlist


def AutomaticShelf(table):
    """Given a SQLAlchemy Table, automatically generate a Shelf with metrics
    and dimensions based on its schema.
    """
    if hasattr(table, "__table__"):
        table = table.__table__
    config = introspect_table(table)
    return Shelf.from_config(config, table)


def introspect_table(table):
    """Given a SQLAlchemy Table object, return a Shelf description suitable
    for passing to Shelf.from_config.
    """
    d = {}
    for c in table.columns:
        if isinstance(c.type, String):
            d[c.name] = {"kind": "Dimension", "field": c.name}
        if isinstance(c.type, (Integer, Float)):
            d[c.name] = {"kind": "Metric", "field": c.name}
    return d
