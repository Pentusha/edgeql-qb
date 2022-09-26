from functools import reduce
from typing import Iterator

from edgeql_qb.expression import Expression
from edgeql_qb.render.expression import render_expression
from edgeql_qb.render.tools import combine_many_renderers, join_renderers
from edgeql_qb.render.types import RenderedQuery


def render_with_expression(
        with_aliases: tuple[Expression, ...],
        generator: Iterator[int],
) -> RenderedQuery:
    if not with_aliases:
        return RenderedQuery()
    renderers = [
        render_expression(alias.to_infix_notation(), 'with', generator)
        for alias in with_aliases
    ]
    return combine_many_renderers(
        RenderedQuery('with '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' '),
    )
