class BadIngredient(Exception):
    """ Something is wrong with an ingredient """


class BadRecipe(Exception):
    """ Something is wrong with a recipe """


class InvalidColumnError(Exception):
    def __init__(self, *args, **kwargs):
        self.column_name = kwargs.pop("column_name", None)
        if not args:
            # default exception message
            args = [f'Invalid column "{self.column_name}"']
        super(InvalidColumnError, self).__init__(*args, **kwargs)


class IngredientNotFoundError(KeyError):
    """Dynamic ingredient can't be found. """
    def __init__(self, message, ingr):
        super().__init__(message)
        self.ingr = ingr
