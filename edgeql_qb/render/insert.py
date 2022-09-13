from functools import reduce, singledispatch
from typing import Iterator

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
    SubQuery,
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


def render_values(values: list[Expression], generator: Iterator[int]) -> RenderedQuery:
    assert values
    renderers = [
        render_insert_expression(value.to_infix_notation(), generator)
        for value in values
    ]
    return combine_many_renderers(
        RenderedQuery(' { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


@singledispatch
def render_insert_expression(expression: AnyExpression, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_insert_expression.register
def _(expression: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_insert_expression(arg, generator)
        for arg in expression.args
    ]
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(f'{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_insert_expression.register
def _(expression: Column, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(expression.column_name)


@render_insert_expression.register
def _(expression: Node, generator: Iterator[int]) -> RenderedQuery:
    assert expression.right is not None, 'Unary operations is not supported in insert expressions'
    return render_binary_node(
        left=render_insert_expression(expression.left, generator),
        right=render_insert_expression(expression.right, generator),
        expression=expression,
    )


@render_insert_expression.register
def _(expression: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'insert_{index}'
    return render_query_literal(expression.value, name)


@render_insert_expression.register
def _(expression: SubQuery, generator: Iterator[int]) -> RenderedQuery:
    return expression.all(generator)
