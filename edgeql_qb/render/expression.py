from functools import singledispatch, reduce

from edgeql_qb.expression import AnyExpression, Expression, QueryLiteral
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Column, Alias, Node
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import join_renderers, combine_many_renderers, render_binary_node
from edgeql_qb.render.types import RenderedQuery


@singledispatch
def render_expression(
    expression: AnyExpression,
    index: int,
    literal_prefix: str,
    column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} {index=} is not supported')  # pragma: no cover


@render_expression.register
def _(
    expression: FuncInvocation,
    index: int,
    literal_prefix: str,
    column_prefix: str = '',
) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_expression(
            Expression(arg).to_infix_notation(index),
            index,
            literal_prefix,
            column_prefix,
        )
        for arg in expression.args
    ]
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_expression.register
def _(
    expression: Column,
    index: int,
    literal_prefix: str,
    column_prefix: str = '',
) -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_expression.register
def _(
    expression: Alias,
    index: int,
    literal_prefix: str,
    column_prefix: str = '',
) -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_expression.register
def _(
    expression: QueryLiteral,
    index: int,
    literal_prefix: str,
    column_prefix: str = '',
) -> RenderedQuery:
    name = f'{literal_prefix}_{expression.query_index}_{index}_{expression.expression_index}'
    return render_query_literal(expression.value, name)


@render_expression.register
def _(
    expression: Node,
    index: int,
    literal_prefix: str,
    column_prefix: str = '',
) -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_expression(expression.left, index, literal_prefix, column_prefix),
        )
    return render_binary_node(
        left=render_expression(
            expression.left,
            index,
            literal_prefix,
            column_prefix=expression.op != ':=' and '.' or ''
        ),
        right=render_expression(expression.right, index, literal_prefix, '.'),
        expression=expression,
    )
