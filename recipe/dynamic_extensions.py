import abc
from stevedore.named import NamedExtensionManager


class DynamicExtensionBase(metaclass=abc.ABCMeta):
    """Base class for dynamic extensions"""

    def __init__(self, recipe_parts, hook_type="modify_query"):
        self.recipe_parts = recipe_parts
        self.hook_type = hook_type

    @abc.abstractmethod
    def execute(self):
        """Perform transformations on recipe_parts here"""
        return self.recipe_parts


def run_hooks(recipe_parts, hook_type, extensions=[]):
    if not extensions:
        return recipe_parts

    namespace = "recipe.hooks." + hook_type
    hook_mgr = NamedExtensionManager(namespace, extensions, name_order=True)

    for extension in hook_mgr.extensions:
        recipe_parts = extension.plugin(recipe_parts).execute()

    return recipe_parts
