from functools import reduce, singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
    SubQueryExpression,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Node
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    join_renderers,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery


def render_insert(model_name: str) -> RenderedQuery:
    return RenderedQuery(f'insert {model_name}')


def render_values(values: list[Expression], literal_index: int) -> RenderedQuery:
    assert values
    renderers = [
        render_insert_expression(value.to_infix_notation(literal_index + index), literal_index)
        for index, value in enumerate(values)
    ]
    return combine_many_renderers(
        RenderedQuery(' { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


@singledispatch
def render_insert_expression(expression: AnyExpression, literal_index: int) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_insert_expression.register
def _(expression: FuncInvocation, literal_index: int) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_insert_expression(arg, literal_index)
        for arg in expression.args
    ]
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(f'{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_insert_expression.register
def _(expression: Column, literal_index: int) -> RenderedQuery:
    return RenderedQuery(expression.column_name)


@render_insert_expression.register
def _(expression: Node, literal_index: int) -> RenderedQuery:
    assert expression.right is not None, 'Unary operations is not supported in insert expressions'
    return render_binary_node(
        left=render_insert_expression(expression.left, literal_index),
        right=render_insert_expression(expression.right, literal_index),
        expression=expression,
    )


@render_insert_expression.register
def _(expression: QueryLiteral, literal_index: int) -> RenderedQuery:
    name = f'insert_{expression.literal_index}'
    return render_query_literal(expression.value, name)


@render_insert_expression.register
def _(expression: SubQueryExpression, literal_index: int) -> RenderedQuery:
    return expression.subquery.all(literal_index=literal_index + 1)
