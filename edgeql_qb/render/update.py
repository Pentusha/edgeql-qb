from functools import reduce, singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Expression,
    QueryLiteral,
    SubQueryExpression,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Column, Node
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    join_renderers,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery


def render_update(model_name: str) -> RenderedQuery:
    return RenderedQuery(f'update {model_name}')


def render_values(values: tuple[Expression, ...], query_index: int) -> RenderedQuery:
    assert values
    renderers = [
        render_update_expression(value.to_infix_notation(query_index + 1), index)
        for index, value in enumerate(values)
    ]
    return combine_many_renderers(
        RenderedQuery(' set { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


@singledispatch
def render_update_expression(expression: AnyExpression, index: int) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} {index=} is not supported')  # pragma: no cover


@render_update_expression.register
def _(expression: Column, index: int) -> RenderedQuery:
    return RenderedQuery(expression.column_name)


@render_update_expression.register
def _(expression: Node, index: int) -> RenderedQuery:
    assert expression.right is not None, 'Unary operations is not supported in update expressions'
    return render_binary_node(
        left=render_update_expression(expression.left, index),
        right=render_update_expression(expression.right, index),
        expression=expression,
    )


@render_update_expression.register
def _(expression: FuncInvocation, index: int) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_update_expression(arg, index)
        for arg in expression.args
    ]
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_update_expression.register
def _(expression: QueryLiteral, index: int) -> RenderedQuery:
    name = f'update_{expression.query_index}_{index}_{expression.expression_index}'
    return render_query_literal(expression.value, name)


@render_update_expression.register
def _(expression: SubQueryExpression, index: int) -> RenderedQuery:
    return expression.subquery.all(expression.index)
