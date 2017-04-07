# TODO as jason about methods of doing this
def recipeextension(func):
    """ A decorator to indicate if this method should be available to the
    recipes builder pattern """

    def decorator(func):
        return


def reg(func):
    for m in dir(func):
        print
        print m
        print getattr(func, m)
    # func.im_class.recipeextensions.add(func.__name__)
    return func


class A(object):
    recipeextensions = set()

    @reg
    def a(self):
        return 1


class RecipeExtension(object):
    """
    Recipe extensions plug into the recipe builder pattern and can modify the
    generated query.

    Methods marked with the decorator ``@recipebuilder`` connect to Recipe's
    builder pattern and parameterize the extension.

    The extension should mark itself as ``dirty`` if it has changes which
    change the current recipe results.

    recipe generates a query in the following way

        (RECIPE) recipe checks its dirty state and all extension dirty states to
        determine if the cached query needs to be regenerated

        (EXTENSIONS) all extension ``add_ingredients`` run to inject
        ingredients directly on the recipe

        (RECIPE) recipe runs gather_all_ingredients_into_cauldron to build a
        global lookup for ingredients

        (RECIPE) recipe runs cauldron.brew_query_parts to gather sqlalchemy
        columns, group_bys and filters

        (EXTENSIONS) all extension ``modify_sqlalchemy(columns,
        group_bys, filters)`` run to directly modify the collected
        sqlalchemy columns, group_bys or filters

        (RECIPE) recipe builds a preliminary query with columns

        (EXTENSIONS) all extension ``modify_sqlalchemy_prequery(query,
        columns, group_bys, filters)`` run to modify the query

        (RECIPE) recipe builds a full query with group_bys, order_bys,
        and filters.

        (RECIPE) recipe tests that this query only uses a single from

        (EXTENSIONS) all extension ``modify_sqlalchemy_postquery(query,
        columns, group_bys, order_bys filters)`` run to modify the query

        (RECIPE) recipe applies limits and offsets on the query

        (RECIPE) recipe caches completed query and sets all dirty flags to
        False.


    When the recipe fetches data the results will be ``enchanted`` to add
    fields to the result. ``RecipeExtensions`` can modify result rows with

        enchant_add_fields: Return a tuple of field names to add to a result row

        enchant_row(row): Return a tuple of field values for each row in
        results.

    """
    recipeextensions = []

    def __init__(self):
        self.dirty = True
        self.recipe = None

    def add_ingedients(self):
        """ This method should be overridden by subclasses """
        pass

    def modify_recipe_parts(self, recipe_parts):
        """ This method allows extensions to directly modify columns,
        group_bys, filters, and order_bys generated from collected
        ingredients. """
        return {
            "columns": recipe_parts['columns'],
            "group_bys": recipe_parts['group_bys'],
            "filters": recipe_parts['filters'],
            "havings": recipe_parts['havings'],
            "order_bys": recipe_parts['order_bys'],
        }

    def modify_prequery_parts(self, prequery_parts):
        """ This method allows extensions to directly modify query,
        group_bys, filters, and order_bys generated from collected
        ingredients after a preliminary query using columns has been created.
        """
        return {
            "query": prequery_parts['query'],
            "group_bys": prequery_parts['group_bys'],
            "filters": prequery_parts['filters'],
            "havings": prequery_parts['havings'],
            "order_bys": prequery_parts['order_bys'],
        }

    def modify_postquery_parts(self, postquery_parts):
        """ This method allows extensions to directly modify query,
        group_bys, filters, and order_bys generated from collected
        ingredients after a final query using columns has been created.
        """
        return {
            "query": postquery_parts['query'],
            "group_bys": postquery_parts['group_bys'],
            "filters": postquery_parts['filters'],
            "havings": postquery_parts['havings'],
            "order_bys": postquery_parts['order_bys'],
        }

    def enchant_add_fields(self):
        """ This method allows extensions to add fields to a result row.
        Return a tuple of the field names that are being added with this method
        """
        return ()

    def enchant_row(self, row):
        """ This method adds the fields named in ``enchant_add_fields`` to
        each result row."""
        return ()


class AutomaticFilters(RecipeExtension):
    """ Add automatic filtering.

    Automatic filters take a dictionary of keys and values. For each key in
    the dictionary, if the
    """

    def __init__(self):
        super(AutomaticFilters, self).__init__()
        self.apply = True
        self.exclude_keys = None
        self.include_keys = None

    @recipeextension
    def apply_automatic_filters(self, value):
        if self.apply != value:
            self.dirty = True
            self.apply = value
        return self.recipe

    @recipeextension
    def exclude_automatic_filter_keys(self, *keys):
        self.exclude_keys = keys

    @recipeextension
    def include_automatic_filter_keys(self, *keys):
        self.include_keys = keys


class UserFilters(RecipeExtension):
    """ Add automatic filtering. """

    def __init__(self):
        super(UserFilters, self).__init__()
        self.compare_recipes = []


class BlendRecipe(RecipeExtension):
    """ Add blend recipes, used for joining data from another table to a base
    table

    Supply a second recipe with a different ``from``
    Optionally supply join criteria, if no join criteria is provided join
    will be attempted using constraints.
    All ingredients from the blended recipe will be hoisted to the base
    recipe except for ingredients that are used for joins (they must be the
    same anyway).

    Supports blend (inner) and full_blend (outer) joins.
    """

    def __init__(self):
        super(BlendRecipe, self).__init__()
        self.compare_recipes = []

    @recipeextension
    def blend(self, blend_recipe):
        return self.recipe

    @recipeextension
    def full_blend(self, blend_recipe):
        return self.recipe


class CompareRecipe(RecipeExtension):
    """ Add compare recipes, used for presenting comparative context
    vis-a-vis a base recipe.

    Supply a second recipe with the same ```from``.
    Metrics from the second recipe will be hoisted to the base recipe and
    suffixed with a string (the default is "_compare"
    Dimensions will be used to match the base recipe to the compare recipe.
    Ordering from the base recipe is maintained.
    """

    def __init__(self):
        super(CompareRecipe, self).__init__()
        self.compare_recipes = []

    @recipeextension
    def compare(self, compare_recipe, suffix='_compare'):
        return self.recipe


class AnonymizeRecipe(RecipeExtension):
    """ Allows recipes to be anonymized by looking at the

    Injects an ingredient.meta._anonymize boolean property on each used
    ingredient.
    """

    def __init__(self):
        super(AnonymizeRecipe, self).__init__()
        self.anonymize = False

    @recipeextension
    def anonymize(self, value):
        """ Should this recipe be anonymized"""
        if self.anonymize != value:
            self.dirty = True
            self.anonymize = value

        # Builder pattern must return the recipe
        return self.recipe

    def modify_sqlalchemy_postquery(self):
        """ Put the anonymizers in the last position of formatters """
        for ingredient in self.recipe._cauldron.ingredients.values():
            if ingredient.meta.anonymizer and self.anonymize:
                ingredient.meta.formatters.append(ingredient.meta.anonymizer)


class CacheRecipe(RecipeExtension):
    pass
