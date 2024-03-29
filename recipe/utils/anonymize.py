import hashlib
import importlib
import string
from inspect import getfullargspec, isclass

import attr
from faker import Faker
from faker.providers import BaseProvider


def generate_faker_seed(value):
    """Generate a seed value for faker."""
    if not isinstance(value, str):
        value = str(value)

    h = hashlib.new("md5")
    h.update(value.encode("utf-8"))
    return int(h.hexdigest()[:16], 16)


class TestProvider(BaseProvider):
    """A demo faker provider for testing string providers"""

    def foo(self):
        return "foo"


class FakerFormatter(string.Formatter):
    """A formatter that can get values from Faker generators."""

    def format_field(self, obj, format_spec):
        """

        :param obj: A faker Faker
        :param format_spec: a generator
        :return: A string generated by
        """
        generator = format_spec
        kwargs = {}
        if "|" in format_spec:
            try:
                newgenerator, potential_kwargs = format_spec.split("|")
                for part in potential_kwargs.split(","):
                    k, v = part.split("=")
                    if v == "None":
                        v = None
                    elif v == "True":
                        v = True
                    elif v == "False":
                        v = False
                    elif v.isdigit():
                        v = int(v)
                    kwargs[k] = v
                generator = newgenerator
            except ValueError:
                # If more than one "|"  don't try to parse
                # If the kwargs aren't of form x=y then don't try to parse
                pass

        value = None
        if callable(getattr(obj, generator)):
            c = getattr(obj, generator)
            argspec = getfullargspec(c)
            if len(argspec.args) == 1:
                value = getattr(obj, generator)()
            elif kwargs:
                value = getattr(obj, generator)(**kwargs)
            else:
                value = c

        if value is not None and not isinstance(value, str):
            value = str(value)
        return value or "Unknown fake generator"


@attr.s
class FakerAnonymizer:
    """Returns a deterministically generated fake value that depends on the
    input value."""

    format_str = attr.ib()
    locale = attr.ib(default="en_US")
    postprocessor = attr.ib(default=None)
    providers = attr.ib(default=None)

    def __attrs_post_init__(self):
        self.fake = Faker(self.locale)
        self.formatter = FakerFormatter()
        for p in self._clean_providers(self.providers):
            self.fake.add_provider(p)

    def _clean_providers(self, providers):
        """Convert a list of anonymizer providers into classes suitable for
        adding with faker.add_provider"""
        if not providers:
            return []

        if not isinstance(providers, (list, tuple)):
            providers = [providers]

        cleaned_providers = []
        for provider in providers:
            if isinstance(provider, str):
                # dynamically import the provider
                parts = provider.split(".")
                if len(parts) > 1:
                    _module = ".".join(parts[:-1])
                    _provider_class = parts[-1]
                    try:
                        _mod = importlib.import_module(_module)
                        _provider = getattr(_mod, _provider_class, None)
                        if _provider is None:
                            # TODO: log an issue, provider not found in module
                            continue
                        elif not issubclass(_provider, BaseProvider):
                            # TODO: log an issue, provider not generator
                            continue
                        else:
                            cleaned_providers.append(_provider)

                    except ImportError:
                        # TODO: log an issue, can't import module
                        continue
            elif isclass(provider) and issubclass(provider, BaseProvider):
                cleaned_providers.append(provider)
            else:
                # TODO: log an issue, provider is not an importable string
                #  or a ProviderBase
                continue

        return cleaned_providers

    def __call__(self, value):
        self.fake.seed_instance(generate_faker_seed(value))
        value = self.formatter.format(self.format_str, fake=self.fake)
        if self.postprocessor is None:
            return value
        else:
            return self.postprocessor(value)
