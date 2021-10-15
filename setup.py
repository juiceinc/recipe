#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

from setuptools import find_packages

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == "publish":
    os.system("python setup.py sdist upload")
    sys.exit()

if sys.argv[-1] == "test":
    try:
        __import__("py")
    except ImportError:
        print("py.test required.")
        sys.exit(1)

    errors = os.system("py.test tests/")
    sys.exit(bool(errors))

# yapf: disable
install = [
    'sqlalchemy>=1.2.2,<1.4',
    'sqlparse',
    'tablib',
    'pyyaml',
    'stevedore',
    'sureberus',
    'faker',
    'dateparser',
    'attrs',
    'lark-parser',
]
# yapf: enable

setup(
    name="recipe",
    version="0.28.0",
    description="A construction kit for SQL",
    long_description=(open("README.rst").read()),
    author="Chris Gemignani",
    author_email="chris.gemignani@juiceanalytics.com",
    url="https://github.com/juiceinc/recipe",
    packages=find_packages(include=["recipe*"]),
    include_package_data=True,
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    tests_require=["pytest", "pytest-cov"],
    install_requires=install,
    entry_points={
        "recipe.oven.drivers": [
            "standard = recipe.oven.drivers.standard_oven:StandardOven"
        ],
        "recipe.hooks.testing": [
            "toyextension2 = tests.test_dynamic_extensions:ToyExtension2"
        ],
    },
)
