from functools import reduce, singledispatch
from typing import Iterator

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    Expression,
    QueryLiteral,
    Shape,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.condition import render_conditions
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    combine_many_renderers,
    combine_renderers,
    join_renderers,
    render_binary_node,
)
from edgeql_qb.render.types import RenderedQuery


def render_select_columns(
        select: tuple[Expression, ...],
        literal_index: int,
        generator: Iterator[int],
) -> RenderedQuery:
    renderers = (
        render_select_expression(
            selectable.to_infix_notation(literal_index=index),
            literal_index,
            generator,
        )
        for index, selectable in enumerate(select)
    )
    return combine_many_renderers(
        RenderedQuery(' { '),
        reduce(join_renderers(', '), renderers),
        RenderedQuery(' }'),
    )


def render_select(
    model_name: str,
    select: tuple[Expression, ...],
    literal_index: int,
    generator: Iterator[int],
    module: str | None = None,
) -> RenderedQuery:
    return combine_renderers(
        RenderedQuery(module and f'with module {module} ' or ''),
        RenderedQuery(f'select {model_name}'),
    ).map(
        lambda r: (
            select
            and combine_renderers(r, render_select_columns(select, literal_index, generator))
            or r
        )
    )


@singledispatch
def render_select_expression(
        expression: AnyExpression,
        literal_index: int,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_select_expression.register
def _(
        expression: Column,
        literal_index: int,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_select_expression.register
def _(
        expression: FuncInvocation,
        literal_index: int,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    func = expression.func
    arg_renderers = [
        render_select_expression(arg, literal_index, generator, column_prefix)
        for arg in expression.args
    ]
    return combine_many_renderers(
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(f'{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_select_expression.register
def _(
        expression: Alias,
        literal_index: int,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_select_expression.register
def _(
        expression: Shape,
        literal_index: int,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    expressions = (
        render_select_expression(exp, literal_index + index, generator, column_prefix)
        for index, exp in enumerate(expression.columns)
    )
    conditions = render_conditions(
        expression.filters,
        generator=generator,
    )
    return combine_many_renderers(
        RenderedQuery(f'{expression.parent.column_name}: {{ '),
        reduce(join_renderers(', '), expressions),
        RenderedQuery(' }'),
        conditions,
    )


@render_select_expression.register
def _(
        expression: QueryLiteral,
        literal_index: int,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    index = next(generator)
    name = f'select_{index}'
    return render_query_literal(expression.value, name)


@render_select_expression.register
def _(
        expression: Node,
        literal_index: int,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    if expression.right is None:
        return combine_many_renderers(
            RenderedQuery(expression.op),
            render_select_expression(expression.left, literal_index, generator, column_prefix),
        )
    return render_binary_node(
        left=render_select_expression(
            expression.left,
            literal_index,
            generator,
            expression.op != ':=' and '.' or '',
        ),
        right=render_select_expression(expression.right, literal_index, generator, '.'),
        expression=expression,
    )
