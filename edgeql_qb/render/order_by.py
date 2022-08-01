from functools import reduce, singledispatch

from edgeql_qb.expression import AnyExpression, Expression, QueryLiteral
from edgeql_qb.operators import Column, Node, SortedExpression
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery


def render_order_by_expressions(
    ordered_by: tuple[Expression, ...],
    query_index: int,
) -> RenderedQuery:
    renderers = (
        render_order_by_expression(expression.to_infix_notation(query_index + 1), index)
        for index, expression in enumerate(ordered_by)
    )
    return combine_renderers(
        RenderedQuery(' order by '),
        reduce(join_renderers(' then '), renderers)
    )


def render_order_by(ordered_by: tuple[Expression, ...], query_index: int) -> RenderedQuery:
    return ordered_by and render_order_by_expressions(ordered_by, query_index) or RenderedQuery()


@singledispatch
def render_order_by_expression(expression: AnyExpression, index: int) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} {index=} is not supported')  # pragma: no cover


@render_order_by_expression.register
def _(expression: Column, index: int) -> RenderedQuery:
    return RenderedQuery(f'.{expression.column_name}')


@render_order_by_expression.register
def _(expression: Node, index: int) -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_order_by_expression(expression.left, index),
        )
    return render_binary_node(
        left=render_order_by_expression(expression.left, index),
        right=render_order_by_expression(expression.right, index),
        expression=expression,
    )


@render_order_by_expression.register
def _(expression: SortedExpression, index: int) -> RenderedQuery:
    return combine_renderers(
        render_order_by_expression(expression.expression, index),
        RenderedQuery(f' {expression.order}'),
    )


@render_order_by_expression.register
def _(expression: QueryLiteral, index: int) -> RenderedQuery:
    name = f'order_by_{index}_{expression.expression_index}'
    return render_query_literal(expression.value, name)
