from functools import reduce, singledispatch

from edgeql_qb.expression import AnyExpression, Expression, QueryLiteral
from edgeql_qb.operators import Column, Node
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    linearize_filter_left,
    render_left_parentheses,
    render_right_parentheses,
)
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import text


def render_conditions(filters: list[Expression], query_index: int) -> RenderedQuery:
    if filters:
        conditions = [
            render_condition(filter_.to_infix_notation(query_index + 1), index)
            for index, filter_ in enumerate(filters)
        ]
        return combine_renderers(
            RenderedQuery(' filter '),
            reduce(join_renderers(' and '), conditions),
        )
    else:
        return RenderedQuery()


@singledispatch
def render_condition(expression: AnyExpression, index: int) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} {index=} is not supported')  # pragma: no cover


@render_condition.register
def _(expression: Node, index: int) -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_condition(expression.left, index),
        )
    right_column = render_condition(expression.right, index)
    right_column = render_right_parentheses(
        expression.right,
        expression,
        right_column
    )
    left_column = render_condition(expression.left, index, )
    left_column = render_left_parentheses(expression.left, expression, left_column)
    return combine_many_renderers(
        left_column,
        RenderedQuery(f' {expression.op} '),
        right_column,
    )


@render_condition.register
def _(expression: QueryLiteral, index: int) -> RenderedQuery:
    name = f'filter_{expression.query_index}_{index}_{expression.expression_index}'
    return render_query_literal(expression.value, name)


@render_condition.register
def _(expression: text, index: int) -> RenderedQuery:
    return RenderedQuery(expression)


@render_condition.register
def _(expression: Column, index: int) -> RenderedQuery:
    columns = linearize_filter_left(expression)
    dot_names = '.'.join(c.column_name for c in columns)
    return RenderedQuery(f'.{dot_names}')
