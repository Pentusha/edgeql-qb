from functools import reduce, singledispatch
from typing import Iterator

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Node
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    linearize_filter_left,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import unsafe_text


def render_filters(
        filters: tuple[Expression, ...],
        literal_index: int,
        generator: Iterator[int],
) -> RenderedQuery:
    conditions = [
        render_condition(
            filter_.to_infix_notation(literal_index + index),
            index,
            literal_index,
            generator,
        )
        for index, filter_ in enumerate(filters)
    ]
    return combine_renderers(
        RenderedQuery(' filter '),
        reduce(join_renderers(' and '), conditions),
    )


def render_conditions(
        filters: tuple[Expression, ...],
        literal_index: int,
        generator: Iterator[int],
) -> RenderedQuery:
    return filters and render_filters(filters, literal_index, generator) or RenderedQuery()


@singledispatch
def render_condition(
        expression: AnyExpression,
        index: int,
        literal_index: int,
        generator: Iterator[int],
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} {index=} is not supported')  # pragma: no cover


@render_condition.register
def _(
        expression: FuncInvocation,
        index: int,
        literal_index: int,
        generator: Iterator[int],
) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_condition(arg, index, literal_index, generator)
        for arg in expression.args
    ]
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(f'{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_condition.register
def _(expression: Node, index: int, literal_index: int, generator: Iterator[int]) -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_condition(expression.left, index, literal_index, generator),
        )
    return render_binary_node(
        left=render_condition(expression.left, index, literal_index, generator),
        right=render_condition(expression.right, index, literal_index, generator),
        expression=expression,
    )


@render_condition.register
def _(
        expression: QueryLiteral,
        index: int,
        literal_index: int,
        generator: Iterator[int],
) -> RenderedQuery:
    index = next(generator)
    name = f'filter_{index}'
    return render_query_literal(expression.value, name)


@render_condition.register
def _(
        expression: unsafe_text,
        index: int,
        literal_index: int,
        generator: Iterator[int],
) -> RenderedQuery:
    return RenderedQuery(expression)


@render_condition.register
def _(
        expression: Column,
        index: int,
        literal_index: int,
        generator: Iterator[int],
) -> RenderedQuery:
    columns = linearize_filter_left(expression)
    dot_names = '.'.join(c.column_name for c in columns)
    return RenderedQuery(f'.{dot_names}')
