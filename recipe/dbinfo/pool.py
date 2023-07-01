from concurrent.futures import ThreadPoolExecutor


class SimpleRecipePool:
    """"""

    def __init__(self, session_factory, recipes):
        self.session_factory = session_factory
        self.recipes = recipes
        self.POOL_MAX = 5

    def get_data(self):
        """Fetch data for each recipe."""
        with ThreadPoolExecutor(max_workers=self.POOL_MAX) as executor:
            return list(executor.map(self._data, self.recipes))

    def _data(self, task):
        if isinstance(task, dict):
            recipe = task["recipe"]
            name = task["name"]
            op = task.get("operation", "all")
        else:
            recipe, name = task
            op = "all"

        # Sessions are not thread-safe, and must be isolated to a specific thread.
        recipe.session(self.session_factory())

        getattr(recipe, op)()

        if isinstance(task, dict):
            return {"recipe": recipe, "name": name, "operation": op}
        else:
            return (recipe, name)
