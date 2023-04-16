from collections.abc import Callable
from dataclasses import dataclass

from edgeql_qb.frozendict import FrozenDict


@dataclass(slots=True, frozen=True)
class RenderedQuery:
    query: str = ''
    context: FrozenDict = FrozenDict()

    def __add__(self, other: 'RenderedQuery') -> 'RenderedQuery':
        return RenderedQuery(
            query=self.query + other.query,
            context=self.context | other.context,
        )

    def map(self, f: Callable[['RenderedQuery'], 'RenderedQuery']) -> 'RenderedQuery':
        return f(self)

    def with_prefix(self, prefix: str) -> 'RenderedQuery':
        return RenderedQuery(prefix) + self

    def with_postfix(self, postfix: str) -> 'RenderedQuery':
        return self + RenderedQuery(postfix)

    def wrap(self, left: str, right: str) -> 'RenderedQuery':
        return self.with_prefix(left).with_postfix(right)
