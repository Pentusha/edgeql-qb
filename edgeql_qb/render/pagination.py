from collections.abc import Iterator
from functools import singledispatch
from typing import Any

from edgeql_qb.expression import QueryLiteral
from edgeql_qb.func import FuncInvocation
from edgeql_qb.render.func import render_generic_function
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import do
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import int64, unsafe_text


@singledispatch
def render_offset(offset: Any, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{offset!r} is not supported')  # pragma: no cover


@render_offset.register
def _(offset: None, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery()


@render_offset.register
def _(offset: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'offset_{index}'
    return render_query_literal(offset.value, name)


@render_offset.register
def _(offset: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    return render_generic_function(
        inp=offset.args,
        closure=do(render_offset, generator=generator),
        func=offset.func,
    ).with_prefix(' offset ')


@render_offset.register
def _(offset: int, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'offset_{index}'
    return render_query_literal(int64(offset), name).with_prefix(' offset ')


@render_offset.register
def _(offset: unsafe_text, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(f' offset {offset!s}')


@singledispatch
def render_limit(limit: Any, generator: Iterator[int]) -> RenderedQuery:
    raise NotImplementedError(f'{limit!r} is not supported')  # pragma: no cover


@render_limit.register
def _(limit: None, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery()


@render_limit.register
def _(limit: int, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'limit_{index}'
    return render_query_literal(int64(limit), name).with_prefix(' limit ')


@render_limit.register
def _(limit: unsafe_text, generator: Iterator[int]) -> RenderedQuery:
    return RenderedQuery(f' limit {limit!s}')


@render_limit.register
def _(limit: QueryLiteral, generator: Iterator[int]) -> RenderedQuery:
    index = next(generator)
    name = f'limit_{index}'
    return render_query_literal(limit.value, name)


@render_limit.register
def _(limit: FuncInvocation, generator: Iterator[int]) -> RenderedQuery:
    return render_generic_function(
        inp=limit.args,
        closure=do(render_limit, generator=generator),
        func=limit.func,
    ).with_prefix(' limit ')
