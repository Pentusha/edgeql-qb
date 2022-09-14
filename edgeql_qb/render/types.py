from dataclasses import dataclass
from typing import Callable

from edgeql_qb.frozendict import FrozenDict


@dataclass(slots=True, frozen=True)
class RenderedQuery:
    query: str = ''
    context: FrozenDict = FrozenDict()

    def map(self, f: Callable[['RenderedQuery'], 'RenderedQuery']) -> 'RenderedQuery':
        return f(self)
