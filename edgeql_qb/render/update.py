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


def render_update(model_name: str) -> RenderedQuery:
    return RenderedQuery(f'update {model_name}')


def render_values(values: tuple[Expression, ...], literal_index: int) -> RenderedQuery:
    assert values
    renderers = [
        render_update_expression(value.to_infix_notation(literal_index + 1), literal_index, '.')
        for index, value in enumerate(values)
    ]
    return combine_many_renderers(
        RenderedQuery(' set { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


@singledispatch
def render_update_expression(
        expression: AnyExpression,
        literal_index: int,
        column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_update_expression.register
def _(expression: Column, literal_index: int, column_prefix: str = '') -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_update_expression.register
def _(expression: Node, literal_index: int, column_prefix: str = '') -> RenderedQuery:
    assert expression.right is not None, 'Unary operations is not supported in update expressions'
    return render_binary_node(
        left=render_update_expression(
            expression.left,
            literal_index,
            expression.op != ':=' and '.' or '',
        ),
        right=render_update_expression(expression.right, literal_index, '.'),
        expression=expression,
    )


@render_update_expression.register
def _(expression: FuncInvocation, literal_index: int, column_prefix: str = '') -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_update_expression(arg, literal_index, column_prefix)
        for arg in expression.args
    ]
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(f'{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_update_expression.register
def _(expression: QueryLiteral, literal_index: int, column_prefix: str = '') -> RenderedQuery:
    name = f'update_{expression.literal_index}'
    return render_query_literal(expression.value, name)


@render_update_expression.register
def _(
        expression: SubQueryExpression,
        literal_index: int,
        column_prefix: str = '',
) -> RenderedQuery:
    return expression.subquery.all(expression.index)
