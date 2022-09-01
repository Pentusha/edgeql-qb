from functools import reduce, singledispatch
from types import NoneType, MappingProxyType
from typing import Any

from edgeql_qb.expression import QueryLiteral, Expression
from edgeql_qb.func import FuncInvocation
from edgeql_qb.render.query_literal import render_query_literal
from edgeql_qb.render.tools import combine_many_renderers, join_renderers
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import unsafe_text


@singledispatch
def render_offset(offset: Any, query_index: int) -> RenderedQuery:
    raise NotImplementedError(f'{offset!r} {query_index=} is not supported')  # pragma: no cover


@render_offset.register
def _(offset: NoneType, query_index: int) -> RenderedQuery:
    return RenderedQuery()


@render_offset.register
def _(offset: QueryLiteral, query_index: int) -> RenderedQuery:
    name = f'offset_{query_index}_{offset.expression_index}'
    return render_query_literal(offset.value, name)


@render_offset.register
def _(offset: FuncInvocation, query_index: int) -> RenderedQuery:
    func = offset.func
    arg_renderers = [
        render_offset(Expression(arg).to_infix_notation(query_index), query_index)
        for arg in offset.args
    ]
    return combine_many_renderers(
        RenderedQuery(' offset '),
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(f'{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )


@render_offset.register
def _(offset: int, query_index: int) -> RenderedQuery:
    name = f'offset_{query_index}'
    return RenderedQuery(f' offset <int64>${name}', MappingProxyType({name: offset}))


@render_offset.register
def _(offset: unsafe_text, query_index: int) -> RenderedQuery:
    return RenderedQuery(f' offset {offset!s}')


@singledispatch
def render_limit(limit: Any, query_index: int) -> RenderedQuery:
    raise NotImplementedError(f'{limit!r} {query_index=} is not supported')  # pragma: no cover


@render_limit.register
def _(limit: NoneType, query_index: int) -> RenderedQuery:
    return RenderedQuery()


@render_limit.register
def _(limit: int, query_index: int) -> RenderedQuery:
    name = f'limit_{query_index}'
    return RenderedQuery(f' limit <int64>${name}', MappingProxyType({name: limit}))


@render_limit.register
def _(limit: unsafe_text, query_index: int) -> RenderedQuery:
    return RenderedQuery(f' limit {limit!s}')


@render_limit.register
def _(limit: QueryLiteral, query_index: int) -> RenderedQuery:
    name = f'limit_{query_index}_{limit.expression_index}'
    return render_query_literal(limit.value, name)


@render_limit.register
def _(limit: FuncInvocation, query_index: int) -> RenderedQuery:
    func = limit.func
    arg_renderers = [
        render_limit(Expression(arg).to_infix_notation(query_index), query_index)
        for arg in limit.args
    ]
    return combine_many_renderers(
        RenderedQuery(' limit '),
        RenderedQuery(f'{func.module}::' if func.module != 'std' else ''),
        RenderedQuery(f'{func.name}('),
        reduce(join_renderers(', '), arg_renderers),
        RenderedQuery(')'),
    )
