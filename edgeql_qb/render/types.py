from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class RenderedQuery:
    query: str = ''
    context: dict[str, Any] = field(default_factory=dict)
