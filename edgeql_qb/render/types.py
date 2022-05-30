from dataclasses import dataclass, field
from typing import Any


@dataclass
class RenderedQuery:
    query: str = ''
    context: dict[str, Any] = field(default_factory=dict)
