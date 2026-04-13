"""
misra_gries.py — Misra-Gries heavy hitter (frequent items) algorithm.

Used to identify which terms/posts dominate the stream and to prune the local
data store by removing entries whose tracked terms have been displaced.

Usage
-----
    from misra_gries import MisraGries

    mg = MisraGries(k=10)          # keep at most k candidates
    for term in stream:
        mg.add(term)
    print(mg.heavy_hitters())      # returns {term: approx_count, ...}
"""

from __future__ import annotations

from typing import Hashable, TypeVar

T = TypeVar("T", bound=Hashable)


class MisraGries:
    """Space-efficient approximation of the top-k frequent items.

    Parameters
    ----------
    k:
        Maximum number of candidates to track.  Any item that appears more
        than ``n / (k + 1)`` times in a stream of length *n* is guaranteed to
        be returned by :py:meth:`heavy_hitters`.
    """

    def __init__(self, k: int) -> None:
        if k < 1:
            raise ValueError("k must be at least 1")
        self._k = k
        self._counts: dict[Hashable, int] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, item: Hashable, count: int = 1) -> None:
        """Process *count* occurrences of *item* from the stream."""
        if count < 1:
            raise ValueError("count must be a positive integer")

        for _ in range(count):
            if item in self._counts:
                self._counts[item] += 1
            elif len(self._counts) < self._k:
                self._counts[item] = 1
            else:
                self._decrement_all()

    def heavy_hitters(self) -> dict[Hashable, int]:
        """Return the current candidate set and their approximate counts."""
        return dict(self._counts)

    def reset(self) -> None:
        """Clear all tracked candidates."""
        self._counts.clear()

    def prune_below(self, threshold: int) -> list[Hashable]:
        """Remove candidates whose count has dropped below *threshold*.

        Returns the list of items that were evicted so callers can delete
        them from persistent storage.
        """
        evicted = [item for item, cnt in self._counts.items() if cnt < threshold]
        for item in evicted:
            del self._counts[item]
        return evicted

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _decrement_all(self) -> None:
        """Decrement every counter by 1 and remove zero-count entries."""
        self._counts = {
            item: cnt - 1
            for item, cnt in self._counts.items()
            if cnt - 1 > 0
        }

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._counts)

    def __repr__(self) -> str:
        return f"MisraGries(k={self._k}, candidates={len(self._counts)})"
