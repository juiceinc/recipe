import threading
import unicodedata

from dogpile.cache import make_region
from dogpile.cache.util import sha1_mangle_key


def async_creation_runner(cache, somekey, creator, mutex):
    """Used by build_region as the async_creation_runner to create keys
    asyncronously. So dogpile.core:Lock when appropriate can avoid blocking.

    :param cache: the cache region we are writing too
    :type key: str
    :param key: the cache key
    :type key: str
    :param key: the creator function
    :type key: obj
    :param key: the mutex for our Lock
    :type key: obj

    :return: Nothing
    """

    def runner():
        try:
            value = creator()
            cache.set(somekey, value)
        finally:
            mutex.release()

    thread = threading.Thread(target=runner)
    thread.start()


def clean_unicode(value):
    """Attempts to normalize a string to a nonunicode value

    :param value: the value to be cleaned of unicode characters
    :type value: str

    :return: a mangled key
    """
    try:
        cleaned_value = str(value)
    except UnicodeEncodeError:
        cleaned_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore")
        if not cleaned_value:
            raise ValueError("Could not find useful chars in the string")
    return cleaned_value


def unicode_sha1_mangle_key(key):
    """Used by mangle_key to sha1 mangle and clean_unicode from a supplied key

    :param key: the value to be mangled and prefixed, typically a SQL query
    :type key: str

    :return: a mangled key
    """
    return sha1_mangle_key(clean_unicode(key))


def mangle_key(key):
    """Used by build_region as the key_mangler for dogpile.cache. It prefixes,
    sha1 mangles, and cleans unicode from a supplied key

    :param key: the value to be mangled and prefixed, typically a SQL query
    :type key: str

    :return: a prefixed and mangled key
    """
    base = "recipe_cache:"
    try:
        prefix, key = key.split(":", 1)
        base += prefix
    except ValueError:
        pass

    return f"{base}:{unicode_sha1_mangle_key(key)}"


def build_region(region_type="redis", region_args={}):
    """An implementation of dogpile.caches make_region that provides a thread
    based async_creation_runner and a prefix sha1 key_mangler.

    :param region_type: a string of the caching backend to use
    :type region_type: str
    :param region_args: a dictionary of configuration values for the backend
    :type region_type: dict

    :return: a cache region object, typically to be stored in recipe settings.
    """
    return make_region(
        async_creation_runner=async_creation_runner, key_mangler=mangle_key
    ).configure("dogpile.cache." + region_type, arguments=region_args)
