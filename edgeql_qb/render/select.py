from functools import reduce, singledispatch
from types import MappingProxyType
from typing import Any

from edgeql_qb.expression import AnyExpression, Expression, QueryLiteral
from edgeql_qb.operators import Alias, Column, Node, SubSelect
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import unsafe_text


def render_select_columns(select: tuple[Expression, ...]) -> RenderedQuery:
    renderers = (
        render_select_expression(selectable.to_infix_notation(), index)
        for index, selectable in enumerate(select)
    )
    return combine_many_renderers(
        RenderedQuery(' { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


def render_select(
    model_name: str,
    select: tuple[Expression, ...],
    module: str | None = None,
) -> RenderedQuery:
    return combine_renderers(
        RenderedQuery(module and f'with module {module} ' or ''),
        RenderedQuery(f'select {model_name}'),
    ).map(lambda r: select and combine_renderers(r, render_select_columns(select)) or r)


@singledispatch
def render_select_expression(
    expression: AnyExpression,
    index: int,
    column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} {index=} is not supported')  # pragma: no cover


@render_select_expression.register
def _(expression: Column, index: int, column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_select_expression.register
def _(expression: Alias, index: int, column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_select_expression.register
def _(expression: SubSelect, index: int, column_prefix: str = '') -> RenderedQuery:
    expressions = (
        render_select_expression(exp, index, column_prefix)
        for exp in expression.columns
    )
    return combine_many_renderers(
        RenderedQuery(f'{expression.parent.column_name}: {{ '),
        reduce(join_renderers(', '), expressions),
        RenderedQuery(' }')
    )


@render_select_expression.register
def _(expression: QueryLiteral, index: int, column_prefix: str = '') -> RenderedQuery:
    name = f'select_{expression.query_index}_{index}_{expression.expression_index}'
    return render_query_literal(expression.value, name)


@render_select_expression.register
def _(expression: Node, index: int, column_prefix: str = '') -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_select_expression(expression.left, index, column_prefix),
        )
    return render_binary_node(
        left=render_select_expression(expression.left, index, expression.op != ':=' and '.' or ''),
        right=render_select_expression(expression.right, index, '.'),
        expression=expression,
    )


@singledispatch
def render_offset(offset: Any, query_index: int) -> RenderedQuery:
    return RenderedQuery()


@render_offset.register
def _(offset: int, query_index: int) -> RenderedQuery:
    name = f'offset_{query_index}'
    return RenderedQuery(f' offset <int64>${name}', MappingProxyType({name: offset}))


@render_offset.register
def _(offset: unsafe_text, query_index: int) -> RenderedQuery:
    return RenderedQuery(f' offset {offset!s}')


@singledispatch
def render_limit(limit: Any, query_index: int) -> RenderedQuery:
    return RenderedQuery()


@render_limit.register
def _(limit: int, query_index: int) -> RenderedQuery:
    name = f'limit_{query_index}'
    return RenderedQuery(f' limit <int64>${name}', MappingProxyType({name: limit}))


@render_limit.register
def _(limit: unsafe_text, query_index: int) -> RenderedQuery:
    return RenderedQuery(f' limit {limit!s}')
