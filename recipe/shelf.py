from collections import OrderedDict
from copy import copy

from six import iteritems
from sqlalchemy import (
    Float, Integer, String, Table, and_, case, distinct, func, or_
)
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql.base import ImmutableColumnCollection
from sqlalchemy.util import lightweight_named_tuple
from yaml import safe_load

from recipe import ingredients
from recipe.compat import basestring
from recipe.exceptions import BadIngredient, BadRecipe
from recipe.ingredients import Dimension, Filter, Ingredient, Metric
from recipe.validators import IngredientValidator

# Ensure case and distinct don't get reaped. We need it in scope for
# creating Metrics
_distinct = distinct
_case = case

_POP_DEFAULT = object()


def ingredient_class_for_name(class_name):
    """Get the class in the recipe.ingredients module with the given name."""
    return getattr(ingredients, class_name, None)


def parse_condition(
    cond, selectable, aggregated=False, default_aggregation='sum'
):
    """Create a SQLAlchemy clause from a condition."""
    if cond is None:
        return None

    else:
        if 'and' in cond:
            conditions = [
                parse_condition(
                    c, selectable, aggregated, default_aggregation
                ) for c in cond['and']
            ]
            return and_(*conditions)
        elif 'or' in cond:
            conditions = [
                parse_condition(
                    c, selectable, aggregated, default_aggregation
                ) for c in cond['or']
            ]
            return or_(*conditions)
        elif 'field' not in cond:
            raise BadIngredient('field must be defined in condition')
        field = parse_field(
            cond['field'],
            selectable,
            aggregated=aggregated,
            default_aggregation=default_aggregation
        )
        if 'in' in cond:
            value = cond['in']
            if isinstance(value, dict):
                raise BadIngredient('value for in must be a list')
            condition_expression = getattr(field, 'in_')(tuple(value))
        elif 'gt' in cond:
            value = cond['gt']
            if isinstance(value, (list, dict)):
                raise BadIngredient('conditional value must be a scalar')
            condition_expression = getattr(field, '__gt__')(value)
        elif 'gte' in cond:
            value = cond['gte']
            if isinstance(value, (list, dict)):
                raise BadIngredient('conditional value must be a scalar')
            condition_expression = getattr(field, '__ge__')(value)
        elif 'lt' in cond:
            value = cond['lt']
            if isinstance(value, (list, dict)):
                raise BadIngredient('conditional value must be a scalar')
            condition_expression = getattr(field, '__lt__')(value)
        elif 'lte' in cond:
            value = cond['lte']
            if isinstance(value, (list, dict)):
                raise BadIngredient('conditional value must be a scalar')
            condition_expression = getattr(field, '__le__')(value)
        elif 'eq' in cond:
            value = cond['eq']
            if isinstance(value, (list, dict)):
                raise BadIngredient('conditional value must be a scalar')
            condition_expression = getattr(field, '__eq__')(value)
        elif 'ne' in cond:
            value = cond['ne']
            if isinstance(value, (list, dict)):
                raise BadIngredient('conditional value must be a scalar')
            condition_expression = getattr(field, '__ne__')(value)
        else:
            raise BadIngredient('Bad condition')

        return condition_expression


def tokenize(s):
    """ Tokenize a string by splitting it by + and -

    >>> tokenize('this + that')
    ['this', 'PLUS', 'that']

    >>> tokenize('this+that')
    ['this', 'PLUS', 'that']

    >>> tokenize('this+that-other')
    ['this', 'PLUS', 'that', 'MINUS', 'other']
    """

    # Crude tokenization
    s = s.replace('+', ' PLUS ').replace('-', ' MINUS ') \
        .replace('/', ' DIVIDE ').replace('*', ' MULTIPLY ')
    words = [w for w in s.split(' ') if w]
    return words


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
    elif hasattr(selectable, 'c'
                ) and isinstance(selectable.c, ImmutableColumnCollection):
        col = getattr(selectable.c, name, None)
        if col is not None:
            return col

        col = _find_in_columncollection(selectable.c, name)
        if col is not None:
            return col

    raise BadIngredient('Can not find {} in {}'.format(name, selectable))


def parse_field(fld, selectable, aggregated=True, default_aggregation='sum'):
    """ Parse a field object from yaml into a sqlalchemy expression """
    # An aggregation is a callable that takes a single field expression
    # None will perform no aggregation
    aggregation_lookup = {
        'sum': func.sum,
        'min': func.min,
        'max': func.max,
        'avg': func.avg,
        'count': func.count,
        'count_distinct': lambda fld: func.count(distinct(fld)),
        'month': lambda fld: func.date_trunc('month', fld),
        'week': lambda fld: func.date_trunc('week', fld),
        'year': lambda fld: func.date_trunc('year', fld),
        'quarter': lambda fld: func.date_trunc('quarter', fld),
        'age': lambda fld: func.date_part('year', func.age(fld)),
        None: lambda fld: fld,
    }

    # Ensure that the dictionary contains:
    # {
    #     'value': str,
    #     'aggregation': str|None,
    #     'condition': dict|None
    # }
    if isinstance(fld, basestring):
        fld = {
            'value': fld,
        }
    if not isinstance(fld, dict):
        raise BadIngredient('fields must be a string or a dict')
    if 'value' not in fld:
        raise BadIngredient('fields must contain a value')
    if not isinstance(fld['value'], basestring):
        raise BadIngredient('field value must be a string')

    # Ensure a condition
    if 'condition' in fld:
        if not isinstance(fld['condition'], dict) and \
                not fld['condition'] is None:
            raise BadIngredient('condition must be null or an object')
    else:
        fld['condition'] = None

    # Ensure an aggregation
    initial_aggregation = default_aggregation if aggregated else None
    if 'aggregation' in fld:
        if not isinstance(fld['aggregation'], basestring) and \
                not fld['aggregation'] is None:
            raise BadIngredient('aggregation must be null or an string')
        if fld['aggregation'] is None:
            fld['aggregation'] = initial_aggregation
    else:
        fld['aggregation'] = initial_aggregation

    value = fld.get('value', None)
    if value is None:
        raise BadIngredient('field value is not defined')

    field_parts = []
    for word in tokenize(value):
        if word in ('MINUS', 'PLUS', 'DIVIDE', 'MULTIPLY'):
            field_parts.append(word)
        else:
            field_parts.append(find_column(selectable, word))

    if len(field_parts) is None:
        raise BadIngredient('field is not defined.')
    # Fields should have an odd number of parts
    if len(field_parts) % 2 != 1:
        raise BadIngredient('field does not have the right number of parts')

    field = field_parts[0]
    if len(field_parts) > 1:
        # if we need to add and subtract from the field
        # join the field parts into pairs, for instance if field parts is
        # [MyTable.first, 'MINUS', MyTable.second, 'PLUS', MyTable.third]
        # we will get two pairs here
        # [('MINUS', MyTable.second), ('PLUS', MyTable.third)]
        for operator, other_field in zip(field_parts[1::2], field_parts[2::2]):
            if operator == 'PLUS':
                field = field.__add__(other_field)
            elif operator == 'MINUS':
                field = field.__sub__(other_field)
            elif operator == 'DIVIDE':
                field = field.__div__(other_field)
            elif operator == 'MULTIPLY':
                field = field.__mul__(other_field)
            else:
                raise BadIngredient('Unknown operator {}'.format(operator))

    # Handle the aggregator
    aggr = fld.get('aggregation', 'sum')
    if aggr is not None:
        aggr = aggr.strip()

    if aggr not in aggregation_lookup:
        raise BadIngredient('unknown aggregation {}'.format(aggr))

    aggregator = aggregation_lookup[aggr]

    condition = parse_condition(
        fld.get('condition', None),
        selectable,
        aggregated=False,
        default_aggregation=default_aggregation
    )

    if condition is not None:
        field = case([(condition, field)])

    return aggregator(field)


def ingredient_from_dict(ingr_dict, selectable):
    """Create an ingredient from an dictionary.

    This object will be deserialized from yaml """

    # TODO: This is deprecated in favor of
    # ingredient_from_validated_dict

    # Describe the required params for each kind of ingredient
    # The key is the parameter name, the value is one of
    # field: A parse_field with aggregation=False
    # aggregated_field: A parse_field with aggregation=True
    # condition: A parse_condition

    params_lookup = {
        'Dimension': {
            'field': 'field'
        },
        'LookupDimension': {
            'field': 'field'
        },
        'IdValueDimension':
            OrderedDict(id_field='field', field='field'),
        'Metric': {
            'field': 'aggregated_field'
        },
        'DivideMetric':
            OrderedDict(
                numerator_field='aggregated_field',
                denominator_field='aggregated_field'
            ),
        'WtdAvgMetric':
            OrderedDict(field='field', weight='field')
    }

    format_lookup = {
        'comma': ',.0f',
        'dollar': '$,.0f',
        'percent': '.0%',
        'comma1': ',.1f',
        'dollar1': '$,.1f',
        'percent1': '.1%',
        'comma2': ',.2f',
        'dollar2': '$,.2f',
        'percent2': '.2%',
    }

    kind = ingr_dict.pop('kind', 'Metric')
    IngredientClass = ingredient_class_for_name(kind)

    if IngredientClass is None:
        raise BadIngredient('Unknown ingredient kind')

    params = params_lookup.get(kind, {'field': 'field'})

    args = []
    for k, v in iteritems(params):
        # All the params must be in the dict
        if k not in ingr_dict:
            raise BadIngredient(
                '{} must be defined to make a {}'.format(k, kind)
            )
        if v == 'field':
            statement = parse_field(
                ingr_dict.pop(k, None), selectable, aggregated=False
            )
        elif v == 'aggregated_field':
            statement = parse_field(
                ingr_dict.pop(k, None), selectable, aggregated=True
            )
        elif v == 'condition':
            statement = parse_condition(
                ingr_dict.pop(k, None), selectable, aggregated=True
            )
        else:
            raise BadIngredient('Do not know what this is')

        args.append(statement)
    # Remaining properties in ingr_dict are treated as keyword args

    # If the format string exists in format_lookup, use the value otherwise
    # use the original format
    if 'format' in ingr_dict:
        ingr_dict['format'] = format_lookup.get(
            ingr_dict['format'], ingr_dict['format']
        )
    return IngredientClass(*args, **ingr_dict)


def parse_validated_field(fld, selectable):
    """ Converts a validated field to sqlalchemy. Field references are
    looked up in selectable """
    aggr_fn = IngredientValidator.aggregation_lookup[fld['aggregation']]

    field = find_column(selectable, fld['value'])

    for operator in fld.get('operators', []):
        op = operator['operator']
        other_field = parse_validated_field(operator['field'], selectable)
        field = IngredientValidator.operator_lookup[op](field)(other_field)

    condition = fld.get('condition', None)
    if condition:
        condition = parse_condition(condition, selectable)
        field = case([(condition, field)])

    field = aggr_fn(field)
    return field


def ingredient_from_validated_dict(ingr_dict, selectable):
    """ Create an ingredient from an dictionary.

    This object will be deserialized from yaml """

    validator = IngredientValidator(schema=ingr_dict['kind'])
    if not validator.validate(ingr_dict):
        raise Exception(validator.errors)
    ingr_dict = validator.document

    kind = ingr_dict.pop('kind', 'Metric')
    IngredientClass = ingredient_class_for_name(kind)

    if IngredientClass is None:
        raise BadIngredient('Unknown ingredient kind')

    args = []
    for fld in ingr_dict.pop('_fields', []):
        args.append(parse_validated_field(ingr_dict.pop(fld), selectable))

    return IngredientClass(*args, **ingr_dict)


class Shelf(object):
    """Holds ingredients used by a recipe.

    Can be initialized with no arguments, but also accepts:
    - a dictionary of ingredients as a positional argument
    - ingredients as keyword arguments

    These keyword arguments have special meaning:
    :param select_from: The SQLALchemy-compatible object which will be queried
        (usually a Table or ORM object).
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

        d = {}
        for k, v in iteritems(obj):
            d[k] = ingredient_constructor(v, selectable)
        shelf = cls(d, select_from=selectable)
        return shelf

    @classmethod
    def from_yaml(cls, yaml_str, selectable, **kwargs):
        """Create a shelf using a yaml shelf definition.

        :param yaml_str: A string containing yaml ingredient definitions.
        :param selectable: A SQLAlchemy Table, a Recipe, or a SQLAlchemy
            join to select from.
        :return: A shelf that contains the ingredients defined in yaml_str.
        """
        obj = safe_load(yaml_str)
        return cls.from_config(
            obj,
            selectable,
            ingredient_constructor=ingredient_from_dict,
            **kwargs
        )

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
