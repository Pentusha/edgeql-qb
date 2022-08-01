from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable


@dataclass(slots=True, frozen=True)
class RenderedQuery:
    query: str = ''
    context: MappingProxyType[str, Any] = MappingProxyType({})

    def map(self, f: Callable[['RenderedQuery'], 'RenderedQuery']) -> 'RenderedQuery':
        return f(self)
