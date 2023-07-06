from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable
import structlog

SLOG = structlog.get_logger(__name__)


class SimplePool:
    """Simple thread pool to fetch data from the database."""

    def __init__(self, callables: List[Callable], pool_max: int = 10):
        """Initialize the pool."""
        self.callables = callables
        self.POOL_MAX = pool_max

    def get_data(self):
        """Fetch data for each recipe."""
        results = []
        log = SLOG.bind(num_callables=len(self.callables))

        with ThreadPoolExecutor(max_workers=self.POOL_MAX) as executor:
            # Run the callables and gather the results.
            future_select = {
                executor.submit(self.call_with_idx, callable, idx)
                for idx, callable in enumerate(self.callables)
            }
            results = []
            for future in as_completed(future_select):
                try:
                    data = future.result()
                    results.append(data)
                except Exception as exc:
                    log.exception("Exception in thread", exc=exc)

        return [data for idx, data in sorted(results)]

    def call_with_idx(self, callable, idx):
        """Helps to return the callables in the original order."""
        return idx, callable()
