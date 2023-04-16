from collections.abc import Iterator
from functools import singledispatch

from edgeql_qb.expression import (
    AnyExpression,
    Column,
    QueryLiteral,
    SubQuery,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.func import render_generic_function
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import do, render_binary_node
from edgeql_qb.render.types import RenderedQuery


@singledispatch
def render_expression(
    expression: AnyExpression,
    literal_prefix: str,
    generator: Iterator[int],
    column_prefix: str = '',
) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_expression.register
def _(
    expression: SubQuery,
    literal_prefix: str,
    generator: Iterator[int],
    column_prefix: str = '',
) -> RenderedQuery:
    return expression.build(generator)


@render_expression.register
def _(
    expression: FuncInvocation,
    literal_prefix: str,
    generator: Iterator[int],
    column_prefix: str = '',
) -> RenderedQuery:
    closure = do(
        render_expression,
        literal_prefix=literal_prefix,
        generator=generator,
        column_prefix=column_prefix,
    )
    return render_generic_function(expression.args, closure, expression.func)


@render_expression.register
def _(
        expression: Column,
        literal_prefix: str,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    return RenderedQuery(f'{column_prefix}{expression.column_name}')


@render_expression.register
def _(
        expression: Alias,
        literal_prefix: str,
        generator: Iterator[int],
        column_prefix: str = '',
) -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_expression.register
def _(
    expression: QueryLiteral,
    literal_prefix: str,
    generator: Iterator[int],
    column_prefix: str = '',
) -> RenderedQuery:
    index = next(generator)
    name = f'{literal_prefix}_{index}'
    return render_query_literal(expression.value, name)


@render_expression.register
def _(
    expression: Node,
    literal_prefix: str,
    generator: Iterator[int],
    column_prefix: str = '',
) -> RenderedQuery:
    if expression.right is None:
        return render_expression(
            expression.left,
            literal_prefix,
            generator,
            column_prefix,
        ).with_prefix(expression.op)
    return render_binary_node(
        left=render_expression(
            expression.left,
            literal_prefix,
            generator,
            column_prefix=expression.op != ':=' and '.' or '',
        ),
        right=render_expression(expression.right, literal_prefix, generator, '.'),
        expression=expression,
    )
