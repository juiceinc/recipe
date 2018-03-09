import importlib
from collections import OrderedDict
from copy import copy

from six import iteritems
from sqlalchemy import Float, Integer, String, case, distinct, func
from sqlalchemy.util import lightweight_named_tuple
from yaml import safe_load

from recipe.compat import basestring
from recipe.exceptions import BadIngredient, BadRecipe
from recipe.ingredients import Dimension, Ingredient, Metric
from recipe.utils import AttrDict

# Ensure case and distinct don't get reaped. We need it in scope for
# creating Metrics
_distinct = distinct
_case = case


def ingredient_class_for_name(class_name):
    # load the module, will raise ImportError if module cannot be loaded
    m = importlib.import_module('recipe.ingredients')
    # get the class, will raise AttributeError if class cannot be found
    c = getattr(m, class_name, None)
    return c


def parse_condition(cond, table, aggregated=False, default_aggregation='sum'):
    """ Create a format string from a condition """
    if cond is None:
        return None

    else:
        if 'field' not in cond:
            raise BadIngredient('field must be defined in condition')
        field = parse_field(
            cond['field'],
            table,
            aggregated=aggregated,
            default_aggregation=default_aggregation
        )
        if 'in' in cond:
            value = cond['in']
            if isinstance(value, (dict)):
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
    ['this', 'PLUS', 'that', 'SUB', 'other]
    """

    # Crude tokenization
    s = s.replace('+', ' PLUS ').replace('-', ' MINUS ') \
        .replace('/', ' DIVIDE ').replace('*', ' MULTIPLY ')
    words = [w for w in s.split(' ') if w]
    return words


def parse_field(fld, table, aggregated=True, default_aggregation='sum'):
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
            if hasattr(table, word):
                field_parts.append(getattr(table, word))
            else:
                raise BadIngredient(
                    '{} is not a field in {}'.format(word, table.__name__)
                )
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
        table=table,
        aggregated=False,
        default_aggregation=default_aggregation
    )

    if condition is not None:
        field = case([(condition, field)])

    return aggregator(field)


def ingredient_from_dict(ingr_dict, table=''):
    """ Create an ingredient from an dictionary.

    This object will be deserialized from yaml """

    # Describe the required params for each kind of ingredient
    # The key is the parameter name, the value is one of
    # field: A parse_field with aggregation=False
    # aggregated_field: A parse_field with aggregation=True
    # condition: A parse_condition
    tablename = table.__name__
    locals()[tablename] = table

    params_lookup = {
        'Dimension': {
            'field': 'field'
        },
        'LookupDimension': {
            'field': 'field'
        },
        'IdValueDimension': {
            'field': 'field',
            'id_field': 'field'
        },
        'Metric': {
            'field': 'aggregated_field'
        },
        'DivideMetric':
            OrderedDict({
                'numerator_field': 'aggregated_field',
                'denominator_field': 'aggregated_field'
            }),
        'WtdAvgMetric':
            OrderedDict({
                'field': 'field',
                'weight': 'field'
            })
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
                ingr_dict.pop(k, None), table, aggregated=False
            )
        elif v == 'aggregated_field':
            statement = parse_field(
                ingr_dict.pop(k, None), table, aggregated=True
            )
        elif v == 'condition':
            statement = parse_condition(
                ingr_dict.pop(k, None), table, aggregated=True
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


class Shelf(AttrDict):
    """ Holds ingredients used by a recipe

    Args:


    Returns:
        A Shelf object
    """

    class Meta:
        anonymize = False
        table = None

    def __init__(self, *args, **kwargs):
        super(Shelf, self).__init__(*args, **kwargs)

        self.Meta.ingredient_order = []
        self.Meta.table = kwargs.pop('table', None)

        # Set the ids of all ingredients on the shelf to the key
        for k, ingredient in self.items():
            ingredient.id = k

    def get(self, k, d=None):
        ingredient = super(Shelf, self).get(k, d)
        if isinstance(ingredient, Ingredient):
            ingredient.id = k
            ingredient.anonymize = self.Meta.anonymize
        return ingredient

    def __getitem__(self, key):
        """ Set the id and anonymize property of the ingredient whenever we
        get or set items """
        ingredient = super(Shelf, self).__getitem__(key)
        ingredient.id = key
        ingredient.anonymize = self.Meta.anonymize
        return ingredient

    def __setitem__(self, key, ingredient):
        """ Set the id and anonymize property of the ingredient whenever we
        get or set items """
        ingredient_copy = copy(ingredient)
        ingredient_copy.id = key
        ingredient_copy.anonymize = self.Meta.anonymize
        super(Shelf, self).__setitem__(key, ingredient_copy)

    def ingredients(self):
        """ Return the ingredients in this shelf in a deterministic order """
        return sorted(list(self.values()))

    @property
    def dimension_ids(self):
        """ Return the Dimensions on this shelf in the order in which
        they were used."""
        return tuple(
            sorted(
                [d.id for d in self.values() if isinstance(d, Dimension)],
                key=
                lambda id: self.Meta.ingredient_order.index(id) if id in self.Meta.ingredient_order else 9999
            )
        )

    @property
    def metric_ids(self):
        """ Return the Metrics on this shelf in the order in which
        they were used. """
        return tuple(
            sorted(
                [d.id for d in self.values() if isinstance(d, Metric)],
                key=
                lambda id: self.Meta.ingredient_order.index(id) if id in self.Meta.ingredient_order else 9999
            )
        )

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
        # Track the order in which ingredients are added.
        self.Meta.ingredient_order.append(ingredient.id)
        self[ingredient.id] = ingredient

    @classmethod
    def from_yaml(cls, yaml_str, table):
        obj = safe_load(yaml_str)
        tablename = table.__name__
        locals()[tablename] = table

        d = {}
        for k, v in iteritems(obj):
            d[k] = ingredient_from_dict(v, table)

        shelf = cls(d)
        shelf.Meta.table = tablename
        return shelf

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
            raise BadRecipe('{} is not a {}'.format(obj, type(filter_to_class)))

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


class AutomaticShelf(Shelf):

    def __init__(self, table, *args, **kwargs):
        d = self._introspect(table)
        super(AutomaticShelf, self).__init__(d)

    def _introspect(self, table):
        """ Build initial shelf using table """
        d = {}
        for c in table.__table__.columns:
            if isinstance(c.type, String):
                d[c.name] = Dimension(c)
            if isinstance(c.type, (Integer, Float)):
                d[c.name] = Metric(func.sum(c))
        return d
