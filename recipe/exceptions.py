class BadIngredient(Exception):
    """ Something is wrong with an ingredient """


class BadRecipe(Exception):
    """ Something is wrong with a recipe """

class InvalidColumnError(Exception):

    def __init__(self, *args, column_name='', **kwargs):
        self.column_name = column_name
        super(InvalidColumnError, self).__init__(*args, **kwargs)
