"""Shared fan-out lifecycle for independent, cancellable jobs.

``web/mb.py``, ``web/discogs.py``, and ``web/routes/browse.py`` each fan out
a handful of independent upstream calls (MusicBrainz browse pages, Discogs
masters/appearances, the MB+Discogs compare skeleton) and want the same
policy: run every job concurrently, but the moment one raises, stop waiting
on the rest and surface that failure immediately rather than after every
sibling call finishes or times out on its own. Before this module existed,
all three had their own copy of the same lifecycle, spelled identically down
to the executor calls; only their surrounding comments differed. One owner
now removes the risk of a silent policy typo drifting between the copies.
"""

import concurrent.futures
from collections.abc import Callable


def parallel_results[Key, Result](
    jobs: dict[Key, Callable[[], Result]], *, max_workers: int,
) -> dict[Key, Result]:
    """Run every job concurrently; surface the first failure without waiting.

    Returns one result per key, in ``jobs``' own key set, once every job has
    succeeded. If any job raises, every other future is cancelled (pending
    ones stop before they start; already-running ones are asked to stop but
    are not waited on) and the executor is torn down without blocking on
    stragglers before the original exception propagates.
    """
    if not jobs:
        return {}
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    futures = {key: executor.submit(job) for key, job in jobs.items()}
    try:
        done, _pending = concurrent.futures.wait(
            futures.values(), return_when=concurrent.futures.FIRST_EXCEPTION,
        )
        # A completed exception wins immediately; do not fall through to the
        # results comprehension below, which would call .result() on a
        # still-running or still-pending sibling that sorts earlier by key
        # order, and block on it before cancellation ever runs.
        for future in done:
            future.result()
        results = {key: future.result() for key, future in futures.items()}
    except BaseException:
        for future in futures.values():
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
        return results
