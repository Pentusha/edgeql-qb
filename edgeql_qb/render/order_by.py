from collections.abc import Iterator
from functools import singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Node, SortedExpression
from edgeql_qb.render.func import render_function
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    do,
    render_binary_node,
    render_many,
)
from edgeql_qb.render.types import RenderedQuery


def render_order_by_expressions(
    ordered_by: tuple[Expression, ...],
    generator: Iterator[int],
) -> RenderedQuery:
    closure = do(render_order_by_expression, generator=generator)
    rendered = render_many(ordered_by, closure, ' then ')
    return rendered.with_prefix(' order by ')


def render_order_by(ordered_by: tuple[Expression, ...], generator: Iterator[int]) -> RenderedQuery:
    return ordered_by and render_order_by_expressions(ordered_by, generator) or RenderedQuery()


@singledispatch
def render_order_by_expression(
        expression: AnyExpression,
        generator: Iterator[int],
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_order_by_expression.register
def _(expression: Column, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(f'.{expression.column_name}')


@render_order_by_expression.register
def _(expression: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    render_expressions = do(render_order_by_expression, generator=generator)
    return (
        expression.args
        @ do(map, render_expressions)
        @ do(list)
        @ do(render_function, expression.func)
    )


@render_order_by_expression.register
def _(expression: Node, generator: Iterator[int]) -> RenderedQuery:
    if expression.right is None:
        return render_order_by_expression(expression.left, generator).with_prefix(expression.op)
    return render_binary_node(
        left=render_order_by_expression(expression.left, generator),
        right=render_order_by_expression(expression.right, generator),
        expression=expression,
    )


@render_order_by_expression.register
def _(expression: SortedExpression, generator: Iterator[int]) -> RenderedQuery:
    rendered: RenderedQuery = (
        expression.expression
        @ do(render_order_by_expression, generator=generator)
    )
    return rendered.with_postfix(f' {expression.order}')


@render_order_by_expression.register
def _(expression: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'order_by_{index}'
    return render_query_literal(expression.value, name)
