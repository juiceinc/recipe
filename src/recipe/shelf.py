from copy import copy
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import func
from sqlalchemy.util import lightweight_named_tuple

from recipe import BadRecipe, Ingredient
from recipe import Dimension
from recipe import Filter
from recipe import Having
from recipe import Metric
from recipe.utils import AttrDict


class Shelf(AttrDict):
    """ Holds ingredients used by a recipe """
    class Meta:
        anonymize = False

    def __init__(self, *args, **kwargs):
        super(Shelf, self).__init__(*args, **kwargs)

        # Set the ids of all ingredients on the shelf to the key
        for k, ingredient in self.iteritems():
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
        def ordering(x):
            # Sort by Dimension,Metric,Filter then by id
            if isinstance(x, Dimension):
                return (0, x.id)
            elif isinstance(x, Metric):
                return (1, x.id)
            elif isinstance(x, Filter):
                return (2, x.id)
            elif isinstance(x, Having):
                return (3, x.id)
            else:
                return (4, x.id)

        values = self.values()
        values.sort(cmp, key=ordering)
        return values

    def __repr__(self):
        """ A string representation of the ingredients used in a recipe
        ordered by Dimensions, Metrics, Filters, then Havings
        """

        def ordering(x):
            # Sort by Dimension,Metric,Filter then by id
            if isinstance(x, Dimension):
                return (0, x.id)
            elif isinstance(x, Metric):
                return (1, x.id)
            elif isinstance(x, Filter):
                return (2, x.id)
            elif isinstance(x, Having):
                return (3, x.id)
            else:
                return (4, x.id)

        lines = []
        # sort the ingredients by type
        for ingredient in self.ingredients():
            lines.append(ingredient.describe())
        return '\n'.join(lines)

    def use(self, ingredient):
        self[ingredient.id] = ingredient

    def find(self, obj, filter_to_class, constructor=None,
             raise_if_invalid=True, apply_sort_order=False):
        """
        Find an Ingredient, optionally using the shelf.

        :param obj: A string or Ingredient
        :param filter_to_class: The Ingredient subclass that obj must be an
        instance of
        :param constructor: An optional callable for building Ingredients
        from obj
        :param raise_if_invalid: Raise an exception if obj is the wrong type
        :param apply_sort_order: If obj is a string prefixed by '-' set the
          found ingredient sort order to descending
        :return: An Ingredient of subclass must_be_type
        """
        if callable(constructor):
            obj = constructor(obj, shelf=self)

        if isinstance(obj, basestring):
            set_descending = False
            if apply_sort_order:
                if obj.startswith('-'):
                    set_descending = True
                    obj = obj[1:]
            if obj not in self:
                if raise_if_invalid:
                    raise BadRecipe("{} doesn't exist on the shelf".format(obj))
                else:
                    return obj

            ingredient = self[obj]
            if set_descending:
                ingredient.ordering = 'desc'

            if not isinstance(ingredient, filter_to_class):
                if raise_if_invalid:
                    raise BadRecipe("{} is not a {}".format(
                        obj, type(filter_to_class)))
                else:
                    return obj
            return ingredient
        elif isinstance(obj, filter_to_class):
            return obj
        else:
            if raise_if_invalid:
                raise BadRecipe("{} is not a {}".format(obj,
                                                        type(filter_to_class)))
            else:
                return obj

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

        return columns, group_bys, filters, havings

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

            for ingredient in self.itervalues():
                if not isinstance(ingredient, (Dimension, Metric)):
                    continue
                if cache_context:
                    ingredient.cache_context = cache_context
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
