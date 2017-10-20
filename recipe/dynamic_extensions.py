import abc

import six
from stevedore.named import NamedExtensionManager


@six.add_metaclass(abc.ABCMeta)
class DynamicExtensionBase(object):
    """Base class for dynamic extensions
    """
    hook_type = 'modify_query'

    def __init__(self, recipe_parts):
        self.recipe_parts = recipe_parts

    @abc.abstractmethod
    def execute(self):
        return self.recipe_parts


def run_hooks(recipe_parts, hook_type, extensions=[]):
    if not extensions:
        return recipe_parts

    namespace = 'recipe.hooks.' + hook_type
    hook_mgr = NamedExtensionManager(namespace, extensions,
                                     # invoke_args=recipe_parts,
                                     # invoke_on_load=True,
                                     name_order=True)

    for extension in hook_mgr.extensions:
        recipe_parts = extension.plugin(recipe_parts).execute()

    return recipe_parts
