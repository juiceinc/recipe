from json import dumps
from recipe.extensions import RecipeExtension, handle_directives
from recipe.utils import recipe_arg


def convert_to_json_encoded(d: dict) -> dict:
    newd = {}
    for k, v in d.items():
        if "," in k and isinstance(v, list):
            v = [itm if isinstance(itm, str) else dumps(itm) for itm in v]
        newd[k] = v
    return newd


class DummyExtension(RecipeExtension):
    recipe_schema = {"a": {"type": "string"}, "a_int": {"type": "integer"}}

    def __init__(self, recipe):
        super().__init__(recipe)
        self.value = None

    @recipe_arg()
    def from_config(self, obj):
        # a_int is in schema but is not handled by a directive
        handle_directives(obj, {"a": self.a})

    @recipe_arg()
    def a(self, value):
        self.value = value

    @recipe_arg()
    def a_int(self, value):
        self.value = str(value * 2)
