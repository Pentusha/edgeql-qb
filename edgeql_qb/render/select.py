from collections.abc import Iterator
from functools import singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
    Shape,
    SubQuery,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.condition import render_conditions
from edgeql_qb.render.func import render_function
from edgeql_qb.render.order_by import render_order_by
from edgeql_qb.render.pagination import render_limit, render_offset
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    do,
    join_with,
    render_binary_node,
    render_many,
    render_parentheses,
)
from edgeql_qb.render.types import RenderedQuery


def render_select_columns(
        select: tuple[Expression, ...],
        generator: Iterator[int],
) -> RenderedQuery:
    closure = do(render_select_expression, generator=generator)
    renderer = render_many(select, closure, ', ')
    return renderer.wrap(' { ', ' }')


def render_select(
    model_name: str,
    select: tuple[Expression, ...],
    generator: Iterator[int],
    select_from_query: SubQuery | None = None,
) -> RenderedQuery:
    rendered_select = (
        select_from_query
        and generator @ do(select_from_query.build) @ do(render_parentheses)
        or RenderedQuery(model_name)
    )
    return rendered_select.with_prefix('select ').map(
        lambda r: (
            select
            and select @ do(render_select_columns, generator=generator) @ do(combine_renderers, r)
            or r
        ),
    )


@singledispatch
def render_select_expression(
        expression: AnyExpression,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_select_expression.register
def _(expression: Column, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_select_expression.register
def _(
        expression: FuncInvocation,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    closure = do(render_select_expression, generator=generator, column_prefix=column_prefix)
    return (
        expression.args
        @ do.each(closure)
        @ do(list)
        @ do(render_function, expression.func)
    )


@render_select_expression.register
def _(expression: Alias, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_select_expression.register
def _(expression: Shape, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    closure = do(render_select_expression, generator=generator, column_prefix=column_prefix)
    conditions = expression.filters @ do(render_conditions, generator=generator)
    order_by = expression.ordered_by @ do(render_order_by, generator=generator)
    rendered_offset = expression.offset_val @ do(render_offset, generator=generator)
    rendered_limit = expression.limit_val @ do(render_limit, generator=generator)
    expressions: RenderedQuery = expression.columns @ do.each(closure) @ join_with(', ')
    return combine_many_renderers(
        expressions.wrap(f'{expression.parent.column_name}: {{ ', ' }'),
        conditions,
        order_by,
        rendered_offset,
        rendered_limit,
    )


@render_select_expression.register
def _(
        expression: QueryLiteral,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    index = next(generator)
    name = f'select_{index}'
    return render_query_literal(expression.value, name)


@render_select_expression.register
def _(expression: Node, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    if expression.right is None:
        return (
            render_select_expression(expression.left, generator, column_prefix)
            .with_prefix(expression.op)
        )
    return render_binary_node(
        left=render_select_expression(
            expression.left,
            generator,
            expression.op != ':=' and '.' or '',
        ),
        right=render_select_expression(expression.right, generator, '.'),
        expression=expression,
    )


@render_select_expression.register
def _(expression: SubQuery, generator: Iterator[int], column_prefix: str = '') -> RenderedQuery:
    return expression.build(generator)
