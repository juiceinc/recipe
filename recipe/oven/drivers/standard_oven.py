from recipe.oven.base import OvenBase


class StandardOven(OvenBase):
    """Concrete Implementation of OvenBase
    """

    def init_engine(self, connection_string=None, **kwargs):
        return super(StandardOven, self).init_engine(connection_string, **kwargs)

    def init_session(self):
        return super(StandardOven, self).init_session()
