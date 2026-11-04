"""Bounded, genuinely concurrent pair scheduling, replaceable by another backend."""

from concurrent.futures import ThreadPoolExecutor


class ThreadExecutionBackend:
    api_version = 1

    def map(self, function, items, *, max_parallel_pairs):
        if type(max_parallel_pairs) is not int or not 1 <= max_parallel_pairs <= 64:
            raise ValueError("max_parallel_pairs must be an integer between 1 and 64")
        with ThreadPoolExecutor(
            max_workers=max_parallel_pairs, thread_name_prefix="pairity"
        ) as executor:
            # Futures execute concurrently; map retains the user's configured pair order.
            return list(executor.map(function, items))
