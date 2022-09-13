from functools import reduce, singledispatch
from typing import Any, Iterator

from edgeql_qb.expression import Column, Expression
from edgeql_qb.operators import Alias, BinaryOp
from edgeql_qb.render.expression import render_expression
from edgeql_qb.render.select import render_select_columns
from edgeql_qb.render.tools import combine_renderers, join_renderers
from edgeql_qb.render.types import RenderedQuery


def render_group(
        model_name: str,
        select: tuple[Expression, ...],
        generator: Iterator[int],
) -> RenderedQuery:
    return (
        RenderedQuery(f'group {model_name}')
        .map(
            lambda r: (
                select
                and combine_renderers(r, render_select_columns(select, generator))
                or r
            )
        )
    )


def render_using(using: tuple[Expression, ...], generator: Iterator[int]) -> RenderedQuery:
    using_expressions = [
        render_expression(use.to_infix_notation(), 'using', generator)
        for use in using
    ]
    return combine_renderers(
        RenderedQuery(' using '),
        reduce(join_renderers(', '), using_expressions),
    )


def render_using_expressions(
        using: tuple[Expression, ...],
        generator: Iterator[int],
) -> RenderedQuery:
    return using and render_using(using, generator) or RenderedQuery()


@singledispatch
def render_group_by(group_by: Any) -> RenderedQuery:
    raise NotImplementedError(f'{group_by!r} is not supported')  # pragma: no cover


@render_group_by.register
def _(group_by: Column) -> RenderedQuery:
    return RenderedQuery(f'.{group_by.column_name}')


@render_group_by.register
def _(group_by: BinaryOp) -> RenderedQuery:
    assert isinstance(group_by.left, Alias)
    return RenderedQuery(group_by.left.name)


def render_group_by_expressions(group_by: tuple[Column, ...]) -> RenderedQuery:
    renderers = map(render_group_by, group_by)
    return combine_renderers(
        RenderedQuery(' by '),
        reduce(join_renderers(', '), renderers),
    )
