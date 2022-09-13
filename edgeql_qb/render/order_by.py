from functools import reduce, singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Node, SortedExpression
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
    literal_index: int,
) -> RenderedQuery:
    renderers = (
        render_order_by_expression(expression.to_infix_notation(literal_index + 1))
        for expression in ordered_by
    )
    return combine_renderers(
        RenderedQuery(' order by '),
        reduce(join_renderers(' then '), renderers)
    )


def render_order_by(ordered_by: tuple[Expression, ...], literal_index: int) -> RenderedQuery:
    return ordered_by and render_order_by_expressions(ordered_by, literal_index) or RenderedQuery()


@singledispatch
def render_order_by_expression(expression: AnyExpression) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_order_by_expression.register
def _(expression: Column) -> RenderedQuery:
    return RenderedQuery(f'.{expression.column_name}')


@render_order_by_expression.register
def _(expression: FuncInvocation) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_order_by_expression(arg)
        for arg in expression.args
    ]
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(f'{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_order_by_expression.register
def _(expression: Node) -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_order_by_expression(expression.left),
        )
    return render_binary_node(
        left=render_order_by_expression(expression.left),
        right=render_order_by_expression(expression.right),
        expression=expression,
    )


@render_order_by_expression.register
def _(expression: SortedExpression) -> RenderedQuery:
    return combine_renderers(
        render_order_by_expression(expression.expression),
        RenderedQuery(f' {expression.order}'),
    )


@render_order_by_expression.register
def _(expression: QueryLiteral) -> RenderedQuery:
    name = f'order_by_{expression.literal_index}'
    return render_query_literal(expression.value, name)
