from collections.abc import Iterator
from functools import singledispatch
from typing import Any

from edgeql_qb.expression import (
    AnyExpression,
    BaseModel,
    Column,
    Expression,
    QueryLiteral,
    SubQuery,
    UnlessConflict,
    UpdateSubQuery,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import Alias, Node
from edgeql_qb.render.func import render_function, render_function_args
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import (
    do,
    render_binary_node,
    render_many,
    render_parentheses,
)
from edgeql_qb.render.types import RenderedQuery


def render_insert(model_name: str) -> RenderedQuery:
    return RenderedQuery(f'insert {model_name}')


def render_values(values: list[Expression], generator: Iterator[int]) -> RenderedQuery:
    assert values
    closure = do(render_insert_expression, generator=generator)
    renderer = render_many(values, closure, ', ')
    return renderer.wrap(' { ', ' }')


@singledispatch
def render_insert_expression(expression: AnyExpression, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{expression!r} is not supported')  # pragma: no cover


@render_insert_expression.register
def _(expression: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    return (
        expression.args
        @ do.each(do(render_insert_expression, generator=generator))
        @ do(list)
        @ do(render_function, expression.func)
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
def _(expression: Alias, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(expression.name)


@render_insert_expression.register
def _(expression: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'insert_{index}'
    return render_query_literal(expression.value, name)


@render_insert_expression.register
def _(expression: SubQuery, generator: Iterator[int]) -> RenderedQuery:
    return expression.build(generator)


@singledispatch
def render_unless_conflict_on(on: Any, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{on!r} is not supported')  # pragma: no cover


@render_unless_conflict_on.register
def _(on: Column, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(f'.{on.column_name}')


@render_unless_conflict_on.register
def _(
    on: tuple,  # type: ignore
    generator: Iterator[int],
) -> RenderedQuery:
    return (
        on
        @ do.each(do(render_unless_conflict_on, generator=generator))
        @ do(render_function_args)  # TODO: It is bug or not?
    )


@singledispatch
def render_unless_conflict(conflict: Any, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{conflict!r} is not supported')  # pragma: no cover


@render_unless_conflict.register
def _(conflict: None, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery()


@render_unless_conflict.register
def _(conflict: UnlessConflict, generator: Iterator[int]) -> RenderedQuery:
    rendered_on = (
        conflict.on
        and render_unless_conflict_on(conflict.on, generator).with_prefix(' on ')
        or RenderedQuery()
    )
    rendered_else = (
        conflict.else_
        and render_unless_conflict_else(conflict.else_, generator).with_prefix(' else ')
        or RenderedQuery()
    )
    return rendered_on.with_prefix(' unless conflict') + rendered_else


@singledispatch
def render_unless_conflict_else(else_: Any, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{else_!r} is not supported')  # pragma: no cover


@render_unless_conflict_else.register
def _(else_: BaseModel, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(else_.name)


@render_unless_conflict_else.register
def _(expression: UpdateSubQuery, generator: Iterator[int]) -> RenderedQuery:
    return generator @ do(expression.build) @ do(render_parentheses)
