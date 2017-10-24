.. _custom_ovens:

===================
Custom Oven Drivers
===================

It's possible to implement your own custom oven drivers to get a desired
behavior for the engine or the session. An abstract base class is provided
for you to inherit from called ``OvenBase``. You need to implement an
``init_engine`` that returns a SQLAlchemy engine, and an ``init_session``
that returns a SQLAlchemy sessionmaker. The default ``__init__`` method
sets the output of both of these to to the oven's ``engine`` and ``Session``
properties respectively.

.. note::  Remember to use recipe's built in settings to handle any
           configuration options/settings you made need for your
           driver.

OvenBase
========

.. currentmodule:: recipe.oven.base
.. autoclass:: OvenBase
   :inherited-members:
