from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import func

from recipe import BadRecipe, Ingredient
from recipe import Dimension
from recipe import Metric
from recipe.utils import AttrDict


class Shelf(AttrDict):
    """ Holds ingredients used by a recipe """

    def __init__(self, *args, **kwargs):
        super(Shelf, self).__init__(*args, **kwargs)

        # Set the ids of all ingredients on the shelf to the key
        for k, ingredient in self.iteritems():
            ingredient.id = k

    def get(self, k, d=None):
        ingredient = super(Shelf, self).get(k, d)
        if isinstance(ingredient, Ingredient):
            ingredient.id = k
        return ingredient

    def __getitem__(self, key):
        ingredient = super(Shelf, self).__getitem__(key)
        ingredient.id = key
        return ingredient

    def __setitem__(self, key, ingredient):
        ingredient.id = key
        super(Shelf, self).__setitem__(key, ingredient)

    def find(self, obj, filter_to_class, constructor=None,
             raise_if_invalid=True):
        """
        Find an Ingredient, optionally using the shelf.

        :param obj: A string or Ingredient
        :param filter_to_class: The Ingredient subclass that obj must be an
        instance of
        :param constructor: An optional callable for building Ingredients
        from obj
        :param raise_if_invalid: Raise an exception if obj is the wrong type
        :return: An Ingredient of subclass must_be_type
        """
        if callable(constructor):
            obj = constructor(obj, shelf=self)

        if isinstance(obj, basestring):
            if obj not in self:
                if raise_if_invalid:
                    raise BadRecipe("{} doesn't exist on the shelf".format(obj))
                else:
                    return obj
            ingredient = self[obj]

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
