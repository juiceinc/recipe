import abc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from recipe import SETTINGS


class OvenBase(metaclass=abc.ABCMeta):
    """Base class for ovens"""

    def __init__(self, connection_string=None):
        self.engine = self.init_engine(connection_string)
        self.Session = self.init_session()

    @abc.abstractmethod
    def init_engine(self, connection_string=None, **kwargs):
        """Initializes a SQLAlchemy Engine for a given connection string with
        all other keyword arguments passed to the create_engine function. The
        connection uses pre-ping to verify connections.

        :param self: a reference to ourselves
        :param connection_string: a reference to ourselves
        :param kwargs: a collection of arguments passed to the engine
        :type self: Oven
        :type connection_string: str
        :type kwargs: dict
        :return: A SQLAlchemy Engine with connection checking
        :rtype: SQLAlchemy.Engine
        """
        if not connection_string:
            return

        connection_settings = {
            "pool_size": SETTINGS.POOL_SIZE,
            "pool_recycle": SETTINGS.POOL_RECYCLE,
            "pool_pre_ping": True,
        }
        connection_settings.update(kwargs)

        engine = create_engine(connection_string, **connection_settings)
        return engine

    @abc.abstractmethod
    def init_session(self):
        """Initializes a SQLAlchemy Session with the Oven's engine

        :param self: a reference to ourselves
        :type self: Oven
        :return: A SQLAlchemy Session using self.engine
        :rtype: SQLAlchemy.Session
        """
        if not self.engine:
            return

        return sessionmaker(bind=self.engine)
