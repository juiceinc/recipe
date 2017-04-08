# TODO as jason about methods of doing this
from copy import copy

from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import text

from recipe import BadRecipe
from recipe import Dimension
from tests.test_base import Base


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

    def __init__(self, recipe):
        self.dirty = True
        self.recipe = recipe

    def add_ingedients(self):
        """
        Add ingredients to the recipe

        This method should be overridden by subclasses """
        pass

    def modify_recipe_parts(self, recipe_parts):
        """
        Modify sqlalchemy components of the query

        This method allows extensions to directly modify columns,
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

    def __init__(self, *args, **kwargs):
        super(AutomaticFilters, self).__init__(*args, **kwargs)
        self.apply = True
        self._automatic_filters = {}
        self.exclude_keys = None
        self.include_keys = None

    def add_ingedients(self):
        if self.apply:
            for dim, values in self._automatic_filters.iteritems():
                operator = None
                if '__' in dim:
                    dim, operator = dim.split('__')
                if self.include_keys is not None and \
                        dim not in self.include_keys:
                    # Ignore keys that are not in include_keys
                    continue

                if self.exclude_keys is not None and \
                        dim in self.exclude_keys:
                    # Ignore keys that are in exclude_keys
                    continue

                # Only look for dimensions
                dimension = self.recipe._shelf.find(dim, Dimension)

                # make a Filter and add it to filters
                self.recipe.filters(dimension.build_filter(values, operator))

    def apply_automatic_filters(self, value):
        if self.apply != value:
            self.dirty = True
            self.apply = value
        return self.recipe

    def automatic_filters(self, value):
        assert isinstance(value, dict)
        self._automatic_filters = value
        self.dirty = True
        return self.recipe

    def exclude_automatic_filter_keys(self, *keys):
        self.exclude_keys = keys
        return self.recipe

    def include_automatic_filter_keys(self, *keys):
        self.include_keys = keys
        return self.recipe


class UserFilters(RecipeExtension):
    """ Add automatic filtering. """

    def __init__(self, *args, **kwargs):
        super(UserFilters, self).__init__(*args, **kwargs)


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

    def __init__(self, *args, **kwargs):
        super(BlendRecipe, self).__init__(*args, **kwargs)
        self.blend_recipes = []


def blend(self, blend_recipe):
    return self.recipe


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

    def __init__(self, *args, **kwargs):
        super(CompareRecipe, self).__init__(*args, **kwargs)
        self.compare_recipe = None
        self.suffix = '_compare'

    def compare(self, compare_recipe, suffix='_compare'):
        self.compare_recipe = compare_recipe
        self.suffix = suffix
        self.dirty = True
        return self.recipe

    def modify_postquery_parts(self, postquery_parts):
        # postquery_parts = {
        #     "query": query,
        #     "group_bys": group_bys,
        #     "filters": filters,
        #     "havings": havings,
        #     "order_bys": order_bys,
        # }
        if self.compare_recipe is None:
            return postquery_parts

        comparison_subq = self.compare_recipe.subquery()

        # For all metrics in the comparison recipe
        # Use the metric in the base recipe and
        # Add the metric columns to the base recipe
        for m in self.compare_recipe.metric_ids:
            met = self.compare_recipe._cauldron[m]
            id = met.id
            met.id = id + self.suffix
            self.recipe._cauldron.use(met)
            for suffix in met.make_column_suffixes():
                col = getattr(comparison_subq.c, id + suffix, None)
                if col is not None:
                    postquery_parts['query'] = postquery_parts[
                        'query'].add_columns(col.label(met.id + suffix))
                else:
                    raise BadRecipe('{} could not be found in .compare() '
                                    'recipe subquery'.format(
                        id + suffix))

        join_conditions = []
        for dim in self.compare_recipe.dimension_ids:
            if dim not in self.recipe.dimension_ids:
                raise BadRecipe('{} dimension in comparison recipe must exist '
                                'in base recipe')
            base_dim = self.recipe._cauldron[dim]
            compare_dim = self.compare_recipe._cauldron[dim]
            base_col = base_dim.columns[0]
            compare_col = getattr(comparison_subq.c, compare_dim.id_prop, None)
            if compare_col is None:
                raise BadRecipe('Can\'t find join property for {} dimension in \
                    compare recipe'.format(compare_dim.id_prop))
            join_conditions.append(base_col == compare_col)

        join_clause = text('1=1')
        if join_conditions:
            join_clause = and_(*join_conditions)

        postquery_parts['query'] = postquery_parts['query'] \
            .outerjoin(comparison_subq, join_clause)

        return postquery_parts

    def _apply_compare_recipes(self, query, order_bys):
        # Apply comparison recipes (a special type of blend recipe where the blend
        # happens through a outer join)
        if self._compare_recipes:
            subq = query.subquery()
            compare_query = self.service.session.query(subq)
            for compare_recipe in self._compare_recipes:
                compare_details = self.build_compare_recipe(subq,
                                                            compare_recipe,
                                                            order_bys)
                compare_query = compare_query.outerjoin(
                    compare_details['subquery'], compare_details['join_clause'])
                compare_query = compare_query.add_columns(
                    *compare_details['added_columns'])

            # If the primary recipe had an ordering use this ordering on the final output
            if order_bys and self._is_postgres():
                compare_query = compare_query.order_by(
                    getattr(subq.c, '_ordering'))
            query = compare_query
        return query

    def build_compare_recipe(self, subq, compare_recipe, order_bys):
        """
        Apply a comparison recipe to the query

        :param subq: A subquery based on the base recipe's query
        :param compare_recipe: The comparison recipe to join
        :param order_bys: A list of order bys to apply to the
        :return: A query
        """
        comparison_subq = compare_recipe.subquery()
        added_columns = []

        # Add all the columns for the current query
        comparison_metrics = compare_recipe._gather(
            compare_recipe.service.metric_shelf,
            compare_recipe._metrics)
        comparison_dimensions = compare_recipe._gather(
            compare_recipe.service.dimension_shelf,
            compare_recipe._dimensions)

        # Promote all the Metrics from the comparison to the base recipe
        # while suffixing them with compare_recipe._compare_suffix
        for ingredient in comparison_metrics:
            new_ingredient = copy(ingredient)
            original_id = new_ingredient.id
            new_ingredient.id += compare_recipe._compare_suffix
            if new_ingredient.id not in self._cauldron.ingredients:
                # Putting the ingredient in the cauldron ensures it will
                # be in the result rows
                # Add it to the _metrics which are used by renderers
                self._cauldron.use(new_ingredient)
                self._metrics += (new_ingredient.id,)
                if original_id in self.service.metric_shelf and \
                        new_ingredient.id not in self.service.metric_shelf:
                    self.service.metric_shelf[new_ingredient.id] = \
                        new_ingredient

                # Find the columns that are used by the metrics in the
                # comparison subquery and add them to the base query columns
                for suffix in new_ingredient.make_column_suffixes():
                    col = getattr(comparison_subq.c, ingredient.id + suffix,
                                  None)
                    if col is not None:
                        added_columns.append(col.label(new_ingredient.id +
                                                       suffix))
                    else:
                        raise BadRecipe('{} could not be found in .compare() '
                                        'recipe subquery'.format(
                            ingredient.id + suffix))

        join_conditions = []
        for dim in comparison_dimensions:
            if dim.id + '_id' not in subq.c:
                raise BadRecipe('When using compare(), the '
                                'recipe\'s dimensions must be used in the '
                                'base recipe.')
            subq_attr = getattr(subq.c, dim.id + '_id', getattr(subq.c, dim.id,
                                                                None))
            comparison_subq_attr = getattr(comparison_subq.c, dim.id + '_id',
                                           getattr(comparison_subq.c, dim.id,
                                                   None))

            if subq_attr is None or comparison_subq_attr is None:
                raise BadRecipe('Attempting to join')

            join_conditions.append(subq_attr == comparison_subq_attr)

        if join_conditions:
            join_clause = and_(*join_conditions)
        else:
            join_clause = text('1=1')

        return {
            'subquery': comparison_subq,
            'join_clause': join_clause,
            'added_columns': added_columns
        }


class AnonymizeRecipe(RecipeExtension):
    """ Allows recipes to be anonymized by adding an anonymize property
    This flips the anonymize flag on all Ingredients used in the recipe.

    Injects an ingredient.meta._anonymize boolean property on each used
    ingredient.

    AnonymizeRecipe should occur last
    """

    def __init__(self, *args, **kwargs):
        super(AnonymizeRecipe, self).__init__(*args, **kwargs)
        self._anonymize = False

    def anonymize(self, value):
        """ Should this recipe be anonymized"""
        assert isinstance(value, bool)

        if self._anonymize != value:
            self.dirty = True
            self._anonymize = value

        # Builder pattern must return the recipe
        return self.recipe

    def add_ingedients(self):
        """ Put the anonymizers in the last position of formatters """
        for ingredient in self.recipe._cauldron.values():
            if hasattr(ingredient.meta, 'anonymizer') and self._anonymize:
                if ingredient.meta.anonymizer not in ingredient.formatters:
                    ingredient.formatters.append(ingredient.meta.anonymizer)


class SummarizeOverRecipe(RecipeExtension):
    def __init__(self, *args, **kwargs):
        super(SummarizeOverRecipe, self).__init__(*args, **kwargs)
        self._summarize_over = None
        self.active = True

    def summarize_over(self, dimension_key):
        self.dirty = True
        self._summarize_over = dimension_key
        return self.recipe

    def add_ingedients(self):
        """
        Take a recipe that has dimensions
        Resummarize it over one of the dimensions returning averages of the
        metrics.
        """
        if self._summarize_over is None:
            return

        if not self.active:
            return

        assert self._summarize_over in self.recipe.dimension_ids

        # Deactivate this summarization to get a clean query
        self.active = False
        base_table = self.recipe.as_table(name='summarized')
        self.active = True

        summmarize_over_dim = self.recipe._cauldron[self._summarize_over]

        # Construct a class dynamically so we can give it a dynamic name
        T = type('T', (Base,), {
            '__table__': base_table,
            '__mapper_args__': {'primary_key': base_table.c.first}
        })

        metrics = []
        for m in self.recipe.metric_ids:
            # Replace the base metric with an averaged version of it
            # targetted to the new table
            base_metric = self.recipe._cauldron[m]

            summarized_metric = copy(base_metric)
            summarized_metric.columns = [func.avg(getattr(T, base_metric.id))]
            metrics.append(summarized_metric)

        dimensions = []
        for dim in self.recipe.dimension_ids:
            if dim != self._summarize_over:
                # Replace the base dimension with a version of it targetted
                # to the new table
                base_dim = self.recipe._cauldron[dim]
                summarized_dim = copy(base_dim)
                summarized_dim.columns = []
                summarized_dim.group_by = []
                for col in base_dim.columns:
                    newcol = getattr(T, col.name)
                    summarized_dim.columns.append(newcol)
                    summarized_dim.group_by.append(newcol)
                dimensions.append(summarized_dim)

        # Rebuild the cauldron using only the dimensions and metrics
        self.recipe._cauldron.clear()
        for met in metrics:
            self.recipe._cauldron.use(met)
        for dim in dimensions:
            self.recipe._cauldron.use(dim)


class Metadata(RecipeExtension):
    pass


class CacheRecipe(RecipeExtension):
    pass
