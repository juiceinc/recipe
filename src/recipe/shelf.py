import importlib
from collections import OrderedDict
from copy import copy
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import func
from sqlalchemy import distinct
from sqlalchemy import case
from sqlalchemy.util import lightweight_named_tuple

from yaml import safe_load

from recipe import BadRecipe, Ingredient, BadIngredient
from recipe import Dimension
from recipe import Metric
from recipe.compat import basestring
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


def parse_condition(cond, table='', aggregated=False,
                    default_aggregation='sum'):
    """ Create a format string from a condition """
    if cond is None:
        return None

    else:
        field = parse_field(cond['field'], table=table, aggregated=aggregated,
                            default_aggregation=default_aggregation)
        if 'in' in cond:
            value = tuple(cond['in'])
            condition_expression = '{field}.in_({value})'.format(**locals())
        elif 'gt' in cond:
            value = cond['gt']
            condition_expression = '{field} > {value}'.format(**locals())
        elif 'lt' in cond:
            value = cond['lt']
            condition_expression = '{field} < {value}'.format(**locals())
        elif 'eq' in cond:
            value = cond['eq']
            condition_expression = '{field} == {value}'.format(**locals())
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
    s = s.replace('+', ' PLUS ').replace('-', ' MINUS ')
    words = [w for w in s.split(' ') if w]
    return words


def parse_field(fld, table='', aggregated=True, default_aggregation='sum'):
    """ Parse a field object from yaml into a sqlalchemy expression """
    aggregation_lookup = {
        'sum': ('func.sum(', ')'),
        'min': ('func.min(', ')'),
        'max': ('func.max(', ')'),
        'count': ('func.count(', ')'),
        'count_distinct': ('func.count(distinct(', '))'),
        'avg': ('func.avg(', ')'),
        None: ('', ''),
    }

    if aggregated:
        initial = {
            'aggregation': default_aggregation,
            'condition': None
        }
    else:
        initial = {
            'aggregation': None,
            'condition': None
        }

    if isinstance(fld, basestring):
        fld = {
            'value': fld,
        }

    initial.update(fld)
    # Ensure that the dictionary contains:
    # {
    #     'value': str,
    #     'aggregation': str|None,
    #     'condition': dict|None
    # }

    value = initial['value']

    field_parts = []
    for word in tokenize(value):
        if word == 'MINUS':
            field_parts.append(' - ')
        elif word == 'PLUS':
            field_parts.append(' + ')
        else:
            field_parts.append('{}.{}'.format(table, word))

    field_str = ''.join(field_parts)

    aggr = initial.get('aggregation', 'sum')
    if aggr is not None:
        aggr = aggr.strip()
    aggregation_prefix, aggregation_suffix = aggregation_lookup[aggr]

    condition = parse_condition(initial.get('condition', None),
                                table=table,
                                aggregated=False,
                                default_aggregation=default_aggregation)

    if condition is None:
        field = field_str
    else:
        field = 'case([({}, {})])'.format(condition, field_str)

    return '{aggregation_prefix}{field}{aggregation_suffix}'.format(
        **locals())


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
        'Dimension': {'field': 'field'},
        'LookupDimension': {'field': 'field'},
        'IdValueDimension': {'field': 'field'},
        'Metric': {'field': 'aggregated_field'},
        'DivideMetric': OrderedDict({
            'numerator_field': 'aggregated_field',
            'denominator_field': 'aggregated_field'}),
        'WtdAvgMetric': OrderedDict({
            'field': 'field',
            'weight': 'field'}),
        'ConditionalMetric': {'field': 'aggregated_field'},
        'SumIfMetric': OrderedDict({
            'field': 'field',
            'condition': 'condition'}),
        'AvgIfMetric': OrderedDict({
            'field': 'field',
            'condition': 'condition'}),
        'CountIfMetric': OrderedDict({
            'field': 'field',
            'condition': 'condition'}),
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
        raise BadIngredient('Bad ingredient kind')

    params = params_lookup.get(kind, {'field': 'field'})

    args = []
    for k, v in params.iteritems():
        # All the params must be in the dict
        if k not in ingr_dict:
            raise BadIngredient('{} must be defined to make a {}'.format(k,
                                                                         kind))
        if v == 'field':
            statement = parse_field(ingr_dict.pop(k, None),
                                    table=tablename,
                                    aggregated=False)
        elif v == 'aggregated_field':
            statement = parse_field(ingr_dict.pop(k, None),
                                    table=tablename,
                                    aggregated=True)
        elif v == 'condition':
            statement = parse_condition(ingr_dict.pop(k, None),
                                        table=tablename,
                                        aggregated=True)
        else:
            raise BadIngredient('Do not know what this is')

        # FIXME: Can we get away from this eval?
        args.append(eval(statement))
    # Remaining properties in ingr_dict are treated as keyword args

    # If the format string exists in format_lookup, use the value otherwise
    # use the original format
    if 'format' in ingr_dict:
        ingr_dict['format'] = format_lookup.get(ingr_dict['format'],
                                                ingr_dict['format'])
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
        return tuple(d.id for d in self.values() if
                     isinstance(d, Dimension))

    @property
    def metric_ids(self):
        return tuple(d.id for d in self.values() if
                     isinstance(d, Metric))

    @property
    def dimension_ids(self):
        """ Return the Dimensions on this shelf in the order in which
        they were used."""
        return tuple(
            sorted(
                [d.id for d in self.values()
                 if isinstance(d, Dimension)],
                key=lambda id: self.Meta.ingredient_order.index(id)
                if id in self.Meta.ingredient_order else 9999
            )
        )

    @property
    def metric_ids(self):
        """ Return the Metrics on this shelf in the order in which
        they were used. """
        return tuple(
            sorted(
                [d.id for d in self.values()
                 if isinstance(d, Metric)],
                key=lambda id: self.Meta.ingredient_order.index(id)
                if id in self.Meta.ingredient_order else 9999
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
        for k, v in obj.iteritems():
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
                raise BadRecipe(
                    "{} doesn't exist on the shelf".format(obj))

            ingredient = self[obj]
            if not isinstance(ingredient, filter_to_class):
                raise BadRecipe('{} is not a {}'.format(
                    obj, filter_to_class))

            if set_descending:
                ingredient.ordering = 'desc'

            return ingredient
        elif isinstance(obj, filter_to_class):
            return obj
        else:
            raise BadRecipe('{} is not a {}'.format(obj,
                                                    type(filter_to_class)))

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
                'result', sample_item._fields + tuple(extra_fields))

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
