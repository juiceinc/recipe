from functools import wraps


def recipe_arg(*args):
    """Decorator for recipe builder arguments.

    Promotes builder pattern by returning self.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *_args, **_kwargs):
            from recipe import Recipe, RecipeExtension, BadRecipe

            if isinstance(self, Recipe):
                recipe = self
            elif isinstance(self, RecipeExtension):
                recipe = self.recipe
            else:
                raise BadRecipe(
                    "recipe_arg can only be applied to"
                    "methods of Recipe or RecipeExtension"
                )

            if recipe._query is not None:
                recipe.reset()

            func(self, *_args, **_kwargs)
            return recipe

        return wrapper

    return decorator
