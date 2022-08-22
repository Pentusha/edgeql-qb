from functools import reduce, singledispatch
from typing import Any

from edgeql_qb.expression import AnyExpression, Expression, QueryLiteral
from edgeql_qb.operators import Alias, BinaryOp, Column, Node
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.select import render_select_columns
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery


def render_group(model_name: str, select: tuple[Expression, ...]) -> RenderedQuery:
    return (
        RenderedQuery(f'group {model_name}')
        .map(lambda r: select and combine_renderers(r, render_select_columns(select)) or r)
    )


@singledispatch
def render_using_expression(
    expression: AnyExpression,
    index: int,
    column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} {index=} is not supported')  # pragma: no cover


@render_using_expression.register
def _(expression: Column, index: int, column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_using_expression.register
def _(expression: Alias, index: int, column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_using_expression.register
def _(expression: QueryLiteral, index: int, column_prefix: str = '') -> RenderedQuery:
    name = f'using_{expression.query_index}_{index}_{expression.expression_index}'
    return render_query_literal(expression.value, name)


@render_using_expression.register
def _(expression: Node, index: int, column_prefix: str = '') -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_using_expression(expression.left, index, column_prefix),
        )
    return render_binary_node(
        left=render_using_expression(expression.left, index, expression.op != ':=' and '.' or ''),
        right=render_using_expression(expression.right, index, '.'),
        expression=expression,
    )


def render_using(using: tuple[Expression, ...]) -> RenderedQuery:
    using_expressions = [
        render_using_expression(use.to_infix_notation(), index)
        for index, use in enumerate(using)
    ]
    return combine_renderers(
        RenderedQuery(' using '),
        reduce(join_renderers(', '), using_expressions),
    )


def render_using_expressions(using: tuple[Expression, ...]) -> RenderedQuery:
    return using and render_using(using) or RenderedQuery()


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
