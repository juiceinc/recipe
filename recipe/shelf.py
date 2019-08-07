import dateparser
from copy import copy, deepcopy

from datetime import date, datetime
from six import iteritems
from sqlalchemy import (
    Float, Integer, String, Table, and_, case, cast, distinct, func, or_
)
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.util import lightweight_named_tuple
from sureberus import errors as E
from sureberus import normalize_schema
from yaml import safe_load

from recipe import ingredients
from recipe.compat import basestring
from recipe.exceptions import BadIngredient, BadRecipe
from recipe.ingredients import Dimension, Filter, Ingredient, Metric
from recipe.schemas import (
    aggregations, condition_schema, ingredient_schema, shelf_schema,
    sqlalchemy_datatypes
)

# Ensure case and distinct don't get reaped. We need it in scope for
# creating Metrics
_distinct = distinct
_case = case

_POP_DEFAULT = object()

# constant used for ensuring safe division
SAFE_DIVISON_EPSILON = 0.000000001


def ingredient_class_for_name(class_name):
    """Get the class in the recipe.ingredients module with the given name."""
    return getattr(ingredients, class_name, None)


def _find_in_columncollection(columns, name):
    """ Find a column in a column collection by name or _label"""
    for col in columns:
        if col.name == name or getattr(col, '_label', None) == name:
            return col
    return None


def find_column(selectable, name):
    """
    Find a column named `name` in selectable

    :param selectable:
    :param name:
    :return: A column object
    """
    from recipe import Recipe

    if isinstance(selectable, Recipe):
        selectable = selectable.subquery()

    # Selectable is a table
    if isinstance(selectable, DeclarativeMeta):
        col = getattr(selectable, name, None)
        if col is not None:
            return col

        col = _find_in_columncollection(selectable.__table__.columns, name)
        if col is not None:
            return col

    # Selectable is a sqlalchemy subquery
    elif hasattr(selectable,
                 'c') and isinstance(selectable.c, ImmutableColumnCollection):
        col = getattr(selectable.c, name, None)
        if col is not None:
            return col

        col = _find_in_columncollection(selectable.c, name)
        if col is not None:
            return col

    raise BadIngredient('Can not find {} in {}'.format(name, selectable))


def _convert_date_value(v):
    parse_kwargs = {
        'languages': ['en'],
    }
    if isinstance(v, date):
        return v
    elif isinstance(v, datetime):
        return v.date()
    elif isinstance(v, basestring):
        parsed_dt = dateparser.parse(v, **parse_kwargs)
        if parsed_dt is None:
            raise ValueError('Could not parse date in {}'.format(v))
        return parsed_dt.date()
    else:
        raise ValueError('Can not convert {} to date'.format(v))


def _convert_datetime_value(v):
    parse_kwargs = {
        'languages': ['en'],
    }
    if isinstance(v, datetime):
        return v
    elif isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    elif isinstance(v, basestring):
        parsed_dt = dateparser.parse(v, **parse_kwargs)
        if parsed_dt is None:
            raise ValueError('Could not parse datetime in {}'.format(v))
        return parsed_dt
    else:
        raise ValueError('Can not convert {} to datetime'.format(v))


def convert_value(field, value):
    """Convert values into something appropriate for this SQLAlchemy data type

    :param field: A SQLAlchemy expression
    :param values: A value or list of values
    """

    if isinstance(value, (list, tuple)):
        if str(field.type) == 'DATE':
            return [_convert_date_value(v) for v in value]
        elif str(field.type) == 'DATETIME':
            return [_convert_datetime_value(v) for v in value]
        else:
            return value
    else:
        if str(field.type) == 'DATE':
            return _convert_date_value(value)
        elif str(field.type) == 'DATETIME':
            return _convert_datetime_value(value)
        else:
            return value


def parse_validated_condition(cond, selectable):
    """ Convert a validated condition into a SQLAlchemy boolean expression """
    if cond is None:
        return

    if 'and' in cond:
        conditions = []
        for c in cond.get('and', []):
            conditions.append(parse_validated_condition(c, selectable))
        return and_(*conditions)

    elif 'or' in cond:
        conditions = []
        for c in cond.get('or', []):
            conditions.append(parse_validated_condition(c, selectable))
        return or_(*conditions)

    elif 'field' in cond:
        field = parse_validated_field(cond.get('field'), selectable)
        _op = cond.get('_op')
        _op_value = convert_value(field, cond.get('_op_value'))

        if _op == 'between':
            return getattr(field, _op)(*_op_value)
        else:
            return getattr(field, _op)(_op_value)


def parse_unvalidated_condition(cond, selectable):
    if cond is None:
        return
    try:
        cond = normalize_schema(condition_schema, cond, allow_unknown=False)
    except E.SureError as e:
        raise BadIngredient(str(e))
    return parse_validated_condition(cond, selectable)


def parse_unvalidated_field(unvalidated_fld, selectable, aggregated=True):
    kind = 'Metric' if aggregated else 'Dimension'
    ingr = {'field': unvalidated_fld, 'kind': kind}
    try:
        ingr_dict = normalize_schema(
            ingredient_schema, ingr, allow_unknown=True
        )
    except E.SureError as e:
        raise BadIngredient(str(e))
    return parse_validated_field(ingr_dict['field'], selectable)


def ingredient_from_unvalidated_dict(unvalidated_ingr, selectable):
    try:
        ingr_dict = normalize_schema(
            ingredient_schema, unvalidated_ingr, allow_unknown=True
        )
    except E.SureError as e:
        raise BadIngredient(str(e))
    return ingredient_from_validated_dict(ingr_dict, selectable)


def parse_validated_field(fld, selectable, use_bucket_labels=True):
    """ Converts a validated field to a sqlalchemy expression.
    Field references are looked up in selectable """
    if fld is None:
        return

    fld = deepcopy(fld)

    if fld.pop('_use_raw_value', False):
        return float(fld['value'])

    if 'buckets' in fld:
        # Buckets only appear in dimensions
        buckets_default_label = fld.get(
            'buckets_default_label'
        ) if use_bucket_labels else 9999
        conditions = [(
            parse_validated_condition(cond, selectable),
            cond.get('label') if use_bucket_labels else idx
        ) for idx, cond in enumerate(fld.get('buckets', []))]
        field = case(conditions, else_=buckets_default_label)
    else:
        field = find_column(selectable, fld['value'])

    operator_lookup = {
        '+': lambda fld: getattr(fld, '__add__'),
        '-': lambda fld: getattr(fld, '__sub__'),
        '/': lambda fld: getattr(fld, '__div__'),
        '*': lambda fld: getattr(fld, '__mul__'),
    }
    for operator in fld.get('operators', []):
        op = operator['operator']
        other_field = parse_validated_field(operator['field'], selectable)
        if op == '/':
            other_field = func.coalesce(cast(other_field, Float), 0.0) \
                + SAFE_DIVISON_EPSILON
        field = operator_lookup[op](field)(other_field)

    # Apply a condition if it exists
    cond = parse_validated_condition(fld.get('condition', None), selectable)
    if cond is not None:
        field = case([(cond, field)])

    # Lookup the aggregation function
    aggr_fn = aggregations.get(fld.get('aggregation'))
    field = aggr_fn(field)

    # lookup the sqlalchemy_datatypes
    cast_to_datatype = sqlalchemy_datatypes.get(fld.get('_cast_to_datatype'))
    if cast_to_datatype is not None:
        field = cast(field, cast_to_datatype)

    coalesce_to_value = fld.get('_coalesce_to_value')
    if coalesce_to_value is not None:
        field = func.coalesce(field, coalesce_to_value)

    return field


def ingredient_from_validated_dict(ingr_dict, selectable):
    """ Create an ingredient from an dictionary.

    This object will be deserialized from yaml """

    kind = ingr_dict.pop('kind', 'Metric')
    IngredientClass = ingredient_class_for_name(kind)

    if IngredientClass is None:
        raise BadIngredient('Unknown ingredient kind')

    field_defn = ingr_dict.pop('field', None)
    divide_by_defn = ingr_dict.pop('divide_by', None)

    field = parse_validated_field(
        field_defn, selectable, use_bucket_labels=True
    )
    if isinstance(field_defn, dict) and 'buckets' in field_defn:
        ingr_dict['order_by_expression'] = parse_validated_field(
            field_defn, selectable, use_bucket_labels=False
        )

    if divide_by_defn is not None:
        # Perform a divide by zero safe division
        divide_by = parse_validated_field(divide_by_defn, selectable)
        field = cast(field, Float) / (
            func.coalesce(cast(divide_by, Float), 0.0) + SAFE_DIVISON_EPSILON
        )

    quickselects = ingr_dict.pop('quickselects', None)
    parsed_quickselects = []
    if quickselects:
        for qf in quickselects:
            parsed_quickselects.append({
                'name':
                    qf['name'],
                'condition':
                    parse_validated_condition(
                        qf.get('condition', None), selectable
                    ),
            })
    ingr_dict['quickselects'] = parsed_quickselects

    args = [field]
    # Each extra field contains a name and a field
    for extra in ingr_dict.pop('extra_fields', []):
        ingr_dict[extra.get('name')] = \
            parse_validated_field(extra.get('field'), selectable)

    return IngredientClass(*args, **ingr_dict)


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
        self.Meta.table = kwargs.pop('table', None)
        self.Meta.select_from = kwargs.pop('select_from', None)
        self.Meta.metadata = kwargs.pop('metadata', None)
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
        """ Set the id and anonymize property of the ingredient whenever we
        get or set items """
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
        """ Set the id and anonymize property of the ingredient whenever we
        get or set items """
        # Maintainer's note: try to make all mutation of self._ingredients go
        # through this method, so we can reliably copy & annotate the
        # ingredients that go into the Shelf.
        if not isinstance(ingredient, Ingredient):
            raise TypeError(
                'Can only set Ingredients as items on Shelf. '
                'Got: {!r}'.format(ingredient)
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
        """ Return the ingredients in this shelf in a deterministic order """
        return sorted(list(self.values()))

    @property
    def dimension_ids(self):
        """ Return the Dimensions on this shelf in the order in which
        they were used."""
        return self._sorted_ingredients([
            d.id for d in self.values() if isinstance(d, Dimension)
        ])

    @property
    def metric_ids(self):
        """ Return the Metrics on this shelf in the order in which
        they were used. """
        return self._sorted_ingredients([
            d.id for d in self.values() if isinstance(d, Metric)
        ])

    @property
    def filter_ids(self):
        """ Return the Metrics on this shelf in the order in which
        they were used. """
        return self._sorted_ingredients([
            d.id for d in self.values() if isinstance(d, Filter)
        ])

    def _sorted_ingredients(self, ingredients):

        def sort_key(id):
            if id in self.Meta.ingredient_order:
                return self.Meta.ingredient_order.index(id)
            else:
                return 9999

        return tuple(sorted(ingredients, key=sort_key))

    def __repr__(self):
        """ A string representation of the ingredients used in a recipe
        ordered by Dimensions, Metrics, Filters, then Havings
        """
        lines = []
        # sort the ingredients by type
        for ingredient in sorted(self.values()):
            lines.append(ingredient.describe())
        return '\n'.join(lines)

    def use(self, ingredient):
        if not isinstance(ingredient, Ingredient):
            raise TypeError(
                'Can only set Ingredients as items on Shelf. '
                'Got: {!r}'.format(ingredient)
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
        metadata=None
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
        elif isinstance(selectable, basestring):
            if '.' in selectable:
                schema, tablename = selectable.split('.')
            else:
                schema, tablename = None, selectable

            selectable = Table(
                tablename,
                metadata,
                schema=schema,
                extend_existing=True,
                autoload=True
            )

        try:
            validated_shelf = normalize_schema(
                shelf_schema, obj, allow_unknown=True
            )
        except E.SureError as e:
            raise BadIngredient(str(e))
        d = {}
        for k, v in iteritems(validated_shelf):
            d[k] = ingredient_constructor(v, selectable)
        shelf = cls(d, select_from=selectable)

        return shelf

    @classmethod
    def from_yaml(cls, yaml_str, selectable, **kwargs):
        """ Shim that calls from_validated_yaml.

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

        if isinstance(obj, basestring):
            set_descending = obj.startswith('-')
            if set_descending:
                obj = obj[1:]

            if obj not in self:
                raise BadRecipe("{} doesn't exist on the shelf".format(obj))

            ingredient = self[obj]
            if not isinstance(ingredient, filter_to_class):
                raise BadRecipe('{} is not a {}'.format(obj, filter_to_class))

            if set_descending:
                ingredient.ordering = 'desc'

            return ingredient
        elif isinstance(obj, filter_to_class):
            return obj
        else:
            raise BadRecipe('{} is not a {}'.format(obj, filter_to_class))

    def brew_query_parts(self):
        """ Make columns, group_bys, filters, havings
        """
        columns, group_bys, filters, havings = [], [], set(), set()
        for ingredient in self.ingredients():
            if ingredient.query_columns:
                columns.extend(ingredient.query_columns)
            if ingredient.group_by:
                group_bys.extend(ingredient.group_by)
            if ingredient.filters:
                filters.update(ingredient.filters)
            if ingredient.havings:
                havings.update(ingredient.havings)

        return {
            'columns': columns,
            'group_bys': group_bys,
            'filters': filters,
            'havings': havings,
        }

    def enchant(self, list, cache_context=None):
        """ Add any calculated values to each row of a resultset generating a
        new namedtuple

        :param list: a list of row results
        :param cache_context: optional extra context for caching
        :return: a list with ingredient.cauldron_extras added for all
                 ingredients
        """
        enchantedlist = []
        if list:
            sample_item = list[0]

            # Extra fields to add to each row
            # With extra callables
            extra_fields, extra_callables = [], []

            for ingredient in self.values():
                if not isinstance(ingredient, (Dimension, Metric)):
                    continue
                if cache_context:
                    ingredient.cache_context += str(cache_context)
                for extra_field, extra_callable in ingredient.cauldron_extras:
                    extra_fields.append(extra_field)
                    extra_callables.append(extra_callable)

            # Mixin the extra fields
            keyed_tuple = lightweight_named_tuple(
                'result', sample_item._fields + tuple(extra_fields)
            )

            # Iterate over the results and build a new namedtuple for each row
            for row in list:
                values = row + tuple(fn(row) for fn in extra_callables)
                enchantedlist.append(keyed_tuple(values))

        return enchantedlist


def AutomaticShelf(table):
    """Given a SQLAlchemy Table, automatically generate a Shelf with metrics
    and dimensions based on its schema.
    """
    if hasattr(table, '__table__'):
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
            d[c.name] = {'kind': 'Dimension', 'field': c.name}
        if isinstance(c.type, (Integer, Float)):
            d[c.name] = {'kind': 'Metric', 'field': c.name}
    return d
