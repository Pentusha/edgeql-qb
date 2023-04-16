from collections.abc import Iterator
from functools import singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.func import render_function
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    do,
    linearize_filter_left,
    render_binary_node,
    render_many,
)
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import unsafe_text


def render_filters(filters: tuple[Expression, ...], generator: Iterator[int]) -> RenderedQuery:
    separator = ' and '
    closure = do(render_condition, generator=generator)
    rendered = render_many(filters, closure, separator)
    return rendered.with_prefix(' filter ')


def render_conditions(filters: tuple[Expression, ...], generator: Iterator[int]) -> RenderedQuery:
    return filters and render_filters(filters, generator) or RenderedQuery()


@singledispatch
def render_condition(expression: AnyExpression, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_condition.register
def _(expression: Alias, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_condition.register
def _(expression: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    return (
        expression.args
        @ do.each(do(render_condition, generator=generator))
        @ do(list)
        @ do(render_function, expression.func)
    )


@render_condition.register
def _(expression: Node, generator: Iterator[int]) -> RenderedQuery:
    if expression.right is None:
        return render_condition(expression.left, generator).with_prefix(expression.op)
    return render_binary_node(
        left=render_condition(expression.left, generator),
        right=render_condition(expression.right, generator),
        expression=expression,
    )


@render_condition.register
def _(expression: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'filter_{index}'
    return render_query_literal(expression.value, name)


@render_condition.register
def _(expression: unsafe_text, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(expression)


@render_condition.register
def _(expression: Column, generator: Iterator[int]) -> RenderedQuery:
    columns = linearize_filter_left(expression)
    dot_names = '.'.join(c.column_name for c in columns)
    return RenderedQuery(f'.{dot_names}')
