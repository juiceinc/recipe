from stevedore import driver


def get_oven(connection_string=None, name="standard"):
    oven = driver.DriverManager(
        namespace="recipe.oven.drivers",
        name=name,
        invoke_on_load=True,
        invoke_args=(connection_string,),
    )
    return oven.driver
