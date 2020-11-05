from unittest.mock import patch
from recipe.dynamic_extensions import DynamicExtensionBase, run_hooks


class ToyExtension(DynamicExtensionBase):
    def execute(self):
        return super(ToyExtension, self).execute()


class ToyExtension2(DynamicExtensionBase):
    name = "TestExtension2"

    def execute(self):
        self.recipe_parts["test"] = 2
        return self.recipe_parts


def test_use_dyanmic_extension_base():
    recipe_parts = {"test": "value"}
    ext = ToyExtension(recipe_parts)
    assert ext.hook_type == "modify_query"
    assert ext.recipe_parts == recipe_parts
    assert ext.execute() == recipe_parts


@patch("recipe.dynamic_extensions.NamedExtensionManager")
def test_run_hooks_no_extensions(nem_patch):
    recipe_parts = {"test": 1}
    assert recipe_parts == run_hooks(recipe_parts, "modify_query")
    assert not nem_patch.called


def test_run_hooks():
    recipe_parts = {"test": 1}
    expected_result = {"test": 2}
    result = run_hooks(recipe_parts, hook_type="testing", extensions=["toyextension2"])
    assert expected_result == result
