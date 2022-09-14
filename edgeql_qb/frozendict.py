from __future__ import annotations
from collections.abc import Mapping

from typing import Any, Iterator


class FrozenDict(Mapping[str, Any]):
    """ Python has no hashable mapping type, so I steel this class from here:
    https://stackoverflow.com/a/2704866
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._d = dict(*args, **kwargs)
        self._hash: int | None = None

    def __iter__(self) -> Iterator[str]:
        return iter(self._d)

    def __len__(self) -> int:
        return len(self._d)

    def __getitem__(self, key: str) -> Any:
        return self._d[key]

    def __or__(self, other: FrozenDict) -> FrozenDict:
        new = dict(self)
        new |= other
        return FrozenDict(new)

    def __hash__(self) -> int:
        if self._hash is None:
            hash_ = 0
            for pair in self.items():
                hash_ ^= hash(pair)
            self._hash = hash_
        return self._hash
