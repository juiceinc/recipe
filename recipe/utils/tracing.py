import time
from contextlib import contextmanager

try:
    from sentry_sdk import start_span
except ImportError:

    @contextmanager
    def start_span(**kwargs):
        # no-op if sentry_sdk isn't installed
        yield


@contextmanager
def trace_performance(log, function_name, **binds):
    with start_span(op=function_name, description=str(binds)):
        start = time.time()
        yield
        log.info(
            "recipe:trace-performance",
            function=function_name,
            duration=time.time() - start,
            **binds
        )
