class BadIngredient(Exception):
    """ Something is wrong with an ingredient """


class BadRecipe(Exception):
    """ Something is wrong with a recipe """


class InvalidColumnError(Exception):
    def __init__(self, *args, **kwargs):
        self.column_name = kwargs.pop("column_name", None)
        if not args:
            # default exception message
            args = ['Invalid column "{}"'.format(self.column_name)]
        super(InvalidColumnError, self).__init__(*args, **kwargs)
