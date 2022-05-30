from functools import reduce
from typing import Callable

from edgeql_qb.expression import SubQueryExpression
from edgeql_qb.operators import Column, Node
from edgeql_qb.render.types import RenderedQuery


def combine_many_renderers(*renderers: RenderedQuery) -> RenderedQuery:
    return reduce(combine_renderers, renderers)


def join_renderers(separator: str = '') -> Callable[[RenderedQuery, RenderedQuery], RenderedQuery]:
    def inner(r1: RenderedQuery, r2: RenderedQuery) -> RenderedQuery:
        return RenderedQuery(
            query=f'{r1.query}{separator}{r2.query}',
            context=r1.context | r2.context,
        )
    return inner


def combine_renderers(r1: RenderedQuery, r2: RenderedQuery) -> RenderedQuery:
    return join_renderers()(r1, r2)


def render_right_parentheses(
    right: Node,
    expression: Node,
    right_column: RenderedQuery,
) -> RenderedQuery:
    if (

        isinstance(right, Node)
        and right < expression
        or (isinstance(right, SubQueryExpression))
        or (isinstance(right, Node) and right == expression and expression.assocright)
        or (getattr(right, 'op', None) == '-' and expression.assocright)
    ):
        right_column = combine_many_renderers(
            RenderedQuery('('),
            right_column,
            RenderedQuery(')'),
        )
    return right_column


def render_left_parentheses(
    left: Node,
    expression: Node,
    left_column: RenderedQuery,
) -> RenderedQuery:
    left_expr_parenthesis = (
        isinstance(left, Node)
        and left < expression
        or (
            left == expression
            and not expression.assocright
            and (
                not isinstance(left, Column)
                and getattr(left, 'operation', 1) != getattr(expression, 'operation', 2)
            )
        )
    )
    if left_expr_parenthesis:
        left_column = combine_many_renderers(
            RenderedQuery('('),
            left_column,
            RenderedQuery(')'),
        )
    return left_column


def linearize_filter_left(column: Column) -> list[Column]:
    match column.parent:
        case None:
            return [column]
        case _:
            return [*linearize_filter_left(column.parent), column]
