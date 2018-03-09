# TODO ask jason about methods of doing this
from sqlalchemy import and_, func, text
from sqlalchemy.ext.declarative import declarative_base

from recipe import BadRecipe, Dimension, Metric, Recipe
from recipe.compat import basestring

Base = declarative_base()


class RecipeExtension(object):
    """
    Recipe extensions plug into the recipe builder pattern and can modify the
    generated query.

    The extension should mark itself as ``dirty`` if it has changes which
    change the current recipe results.

    recipe generates a query in the following way

        (RECIPE) recipe checks its dirty state and all extension dirty
        states to determine if the cached query needs to be regenerated

        (EXTENSIONS) all extension ``add_ingredients`` run to inject
        ingredients directly on the recipe

        (RECIPE) recipe runs gather_all_ingredients_into_cauldron to build a
        global lookup for ingredients

        (RECIPE) recipe runs cauldron.brew_query_parts to gather sqlalchemy
        columns, group_bys and filters

        (EXTENSIONS) all extension ``modify_recipe_parts(recipeparts)`` run to
        directly modify the collected sqlalchemy columns, group_bys or filters

        (RECIPE) recipe builds a preliminary query with columns

        (EXTENSIONS) all extension ``modify_prequery_parts(prequery_parts)``
        run to modify the query

        (RECIPE) recipe builds a full query with group_bys, order_bys,
        and filters.

        (RECIPE) recipe tests that this query only uses a single from

        (EXTENSIONS) all extension ``modify_postquery_parts(
        postquery_parts)`` run to modify the query

        (RECIPE) recipe applies limits and offsets on the query

        (RECIPE) recipe caches completed query and sets all dirty flags to
        False.


    When the recipe fetches data the results will be ``enchanted`` to add
    fields to the result. ``RecipeExtensions`` can modify result rows with

        enchant_add_fields: Return a tuple of field names to add to a
        result row

        enchant_row(row): Return a tuple of field values for each row in
        results.

    """

    def __init__(self, recipe):
        self.dirty = True
        self.recipe = recipe

    def add_ingredients(self):
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
            'columns': recipe_parts['columns'],
            'group_bys': recipe_parts['group_bys'],
            'filters': recipe_parts['filters'],
            'havings': recipe_parts['havings'],
            'order_bys': recipe_parts['order_bys'],
        }

    def modify_prequery_parts(self, prequery_parts):
        """ This method allows extensions to directly modify query,
        group_bys, filters, and order_bys generated from collected
        ingredients after a preliminary query using columns has been created.
        """
        return {
            'query': prequery_parts['query'],
            'group_bys': prequery_parts['group_bys'],
            'filters': prequery_parts['filters'],
            'havings': prequery_parts['havings'],
            'order_bys': prequery_parts['order_bys'],
        }

    def modify_postquery_parts(self, postquery_parts):
        """ This method allows extensions to directly modify query,
        group_bys, filters, and order_bys generated from collected
        ingredients after a final query using columns has been created.
        """
        return {
            'query': postquery_parts['query'],
            'group_bys': postquery_parts['group_bys'],
            'filters': postquery_parts['filters'],
            'havings': postquery_parts['havings'],
            'order_bys': postquery_parts['order_bys'],
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

    def add_ingredients(self):
        if self.apply:
            for dim, values in self._automatic_filters.items():
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


class SummarizeOver(RecipeExtension):

    def __init__(self, *args, **kwargs):
        super(SummarizeOver, self).__init__(*args, **kwargs)
        self._summarize_over = None

    def summarize_over(self, dimension_key):
        self.dirty = True
        self._summarize_over = dimension_key
        return self.recipe

    def modify_postquery_parts(self, postquery_parts):
        """
        Take a recipe that has dimensions
        Resummarize it over one of the dimensions returning averages of the
        metrics.
        """
        if self._summarize_over is None:
            return postquery_parts
        assert self._summarize_over in self.recipe.dimension_ids

        # Start with a subquery
        subq = postquery_parts['query'].subquery(name='summarize')

        summarize_over_dim = set((
            self._summarize_over, self._summarize_over + '_id',
            self._summarize_over + '_raw'
        ))
        dim_column_names = set(dim for dim in self.recipe.dimension_ids).union(
            set(dim + '_id' for dim in self.recipe.dimension_ids)
        ).union(set(dim + '_raw' for dim in self.recipe.dimension_ids))
        used_dim_column_names = dim_column_names - summarize_over_dim

        # Build a new query around the subquery
        group_by_columns = [
            col for col in subq.c if col.name in used_dim_column_names
        ]

        # Generate columns for the metric, remapping the aggregation function
        # count -> sum
        # sum -> sum
        # avg -> avg
        # Metrics can override the summary aggregation by providing a
        # metric.meta.summary_aggregation callable parameter
        metric_columns = []
        for col in subq.c:
            if col.name not in dim_column_names:
                met = self.recipe._cauldron.find(col.name, Metric)
                summary_aggregation = met.meta.get('summary_aggregation', None)
                if summary_aggregation is None:
                    if str(met.expression).startswith(u'avg'):
                        summary_aggregation = func.avg
                    elif str(met.expression).startswith(u'count'):
                        summary_aggregation = func.sum
                    elif str(met.expression).startswith(u'sum'):
                        summary_aggregation = func.sum

                if summary_aggregation is None:
                    # We don't know how to aggregate this metric in a summary
                    raise BadRecipe(
                        u'Provide a summary_aggregation for metric'
                        u' {}'.format(col.name)
                    )
                metric_columns.append(summary_aggregation(col).label(col.name))

        # Find the ordering columns and apply them to the new query
        order_by_columns = []
        for col in postquery_parts['query']._order_by:
            subq_col = getattr(
                subq.c, col.name, getattr(subq.c, col.name + '_raw', None)
            )
            if subq_col is not None:
                order_by_columns.append(subq_col)

        postquery_parts['query'] = self.recipe._session.query(
            *(group_by_columns + metric_columns)
        ).group_by(*group_by_columns).order_by(*order_by_columns)

        # Remove the summarized dimension
        self.recipe._cauldron.pop(self._summarize_over, None)
        return postquery_parts


class Anonymize(RecipeExtension):
    """ Allows recipes to be anonymized by adding an anonymize property
    This flips the anonymize flag on all Ingredients used in the recipe.

    Injects an ingredient.meta._anonymize boolean property on each used
    ingredient.

    AnonymizeRecipe should occur last
    """

    def __init__(self, *args, **kwargs):
        super(Anonymize, self).__init__(*args, **kwargs)
        self._anonymize = False

    def anonymize(self, value):
        """ Should this recipe be anonymized"""
        assert isinstance(value, bool)

        if self._anonymize != value:
            self.dirty = True
            self._anonymize = value

        # Builder pattern must return the recipe
        return self.recipe

    def add_ingredients(self):
        """ Put the anonymizers in the last position of formatters """
        for ingredient in self.recipe._cauldron.values():
            if hasattr(ingredient.meta, 'anonymizer'):
                if ingredient.meta.anonymizer not in ingredient.formatters \
                        and self._anonymize:
                    ingredient.formatters.append(ingredient.meta.anonymizer)
                if ingredient.meta.anonymizer in ingredient.formatters \
                        and not self._anonymize:
                    ingredient.formatters.remove(ingredient.meta.anonymizer)


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
        self.blend_types = []
        self.blend_criteria = []

    def blend(self, blend_recipe, join_base, join_blend):
        assert isinstance(blend_recipe, Recipe)

        self.blend_recipes.append(blend_recipe)
        self.blend_types.append('inner')
        self.blend_criteria.append((join_base, join_blend))
        self.dirty = True
        return self.recipe

    def full_blend(self, blend_recipe, join_base, join_blend):
        assert isinstance(blend_recipe, Recipe)

        self.blend_recipes.append(blend_recipe)
        self.blend_types.append('outer')
        self.blend_criteria.append((join_base, join_blend))
        self.dirty = True
        return self.recipe

    def modify_postquery_parts(self, postquery_parts):
        """
        Make the comparison recipe a subquery that is left joined to the
        base recipe using dimensions that are shared between the recipes.

        Hoist the metric from the comparison recipe up to the base query
        while adding the suffix.

        """
        if not self.blend_recipes:
            return postquery_parts

        for blend_recipe, blend_type, blend_criteria in \
            zip(self.blend_recipes,
                self.blend_types,
                self.blend_criteria):
            join_base, join_blend = blend_criteria

            blend_subq = blend_recipe.subquery()

            # For all metrics in the blend recipe
            # Use the metric in the base recipe and
            # Add the metric columns to the base recipe
            for m in blend_recipe.metric_ids:
                met = blend_recipe._cauldron[m]
                self.recipe._cauldron.use(met)
                for suffix in met.make_column_suffixes():
                    col = getattr(blend_subq.c, met.id, None)
                    if col is not None:
                        postquery_parts['query'
                                       ] = postquery_parts['query'].add_columns(
                                           col.label(met.id + suffix)
                                       )
                    else:
                        raise BadRecipe(
                            '{} could not be found in .blend() '
                            'recipe subquery'.format(id + suffix)
                        )

            # For all dimensions in the blend recipe
            # Use the dimension in the base recipe and
            # Add the dimension columns and group_by to the base recipe
            # Ignore the join_blend dimension
            for d in blend_recipe.dimension_ids:
                if d == join_blend:
                    continue
                dim = blend_recipe._cauldron[d]
                self.recipe._cauldron.use(dim)
                for suffix in dim.make_column_suffixes():
                    col = getattr(blend_subq.c, dim.id, None)
                    if col is not None:
                        postquery_parts['query'
                                       ] = postquery_parts['query'].add_columns(
                                           col.label(dim.id + suffix)
                                       )
                        postquery_parts['query'] = postquery_parts[
                            'query'
                        ].group_by(col)
                    else:
                        raise BadRecipe(
                            '{} could not be found in .blend() '
                            'recipe subquery'.format(id + suffix)
                        )

            base_dim = self.recipe._cauldron[join_base]
            blend_dim = blend_recipe._cauldron[join_blend]

            base_col = base_dim.columns[0]
            blend_col = getattr(blend_subq.c, blend_dim.id_prop, None)
            if blend_col is None:
                raise BadRecipe(
                    'Can\'t find join property for {} dimension in \
                        blend recipe'.format(blend_dim.id_prop)
                )

            if blend_type == 'outer':
                postquery_parts['query'] = postquery_parts['query'] \
                    .outerjoin(blend_subq, base_col == blend_col)
            else:
                postquery_parts['query'] = postquery_parts['query'] \
                    .join(blend_subq, base_col == blend_col)

        return postquery_parts


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
        self.compare_recipe = []
        self.suffix = []

    def compare(self, compare_recipe, suffix='_compare'):
        assert isinstance(compare_recipe, Recipe)
        assert isinstance(suffix, basestring)
        self.compare_recipe.append(compare_recipe)
        self.suffix.append(suffix)
        self.dirty = True
        return self.recipe

    def modify_postquery_parts(self, postquery_parts):
        """
        Make the comparison recipe a subquery that is left joined to the
        base recipe using dimensions that are shared between the recipes.

        Hoist the metric from the comparison recipe up to the base query
        while adding the suffix.

        """
        if not self.compare_recipe:
            return postquery_parts

        for compare_recipe, compare_suffix in zip(
            self.compare_recipe, self.suffix
        ):
            comparison_subq = compare_recipe.subquery()

            # For all metrics in the comparison recipe
            # Use the metric in the base recipe and
            # Add the metric columns to the base recipe
            for m in compare_recipe.metric_ids:
                met = compare_recipe._cauldron[m]
                id = met.id
                met.id = id + compare_suffix
                self.recipe._cauldron.use(met)
                for suffix in met.make_column_suffixes():
                    col = getattr(comparison_subq.c, id + suffix, None)
                    if col is not None:
                        postquery_parts['query'
                                       ] = postquery_parts['query'].add_columns(
                                           col.label(met.id + suffix)
                                       )
                    else:
                        raise BadRecipe(
                            '{} could not be found in .compare() '
                            'recipe subquery'.format(id + suffix)
                        )

            join_conditions = []
            for dim in compare_recipe.dimension_ids:
                if dim not in self.recipe.dimension_ids:
                    raise BadRecipe(
                        '{} dimension in comparison recipe must exist '
                        'in base recipe'
                    )
                base_dim = self.recipe._cauldron[dim]
                compare_dim = compare_recipe._cauldron[dim]
                base_col = base_dim.columns[0]
                compare_col = getattr(
                    comparison_subq.c, compare_dim.id_prop, None
                )
                if compare_col is None:
                    raise BadRecipe(
                        'Can\'t find join property for {} dimension in \
                        compare recipe'.format(compare_dim.id_prop)
                    )
                join_conditions.append(base_col == compare_col)

            join_clause = text('1=1')
            if join_conditions:
                join_clause = and_(*join_conditions)

            postquery_parts['query'] = postquery_parts['query'] \
                .outerjoin(comparison_subq, join_clause)

        return postquery_parts
