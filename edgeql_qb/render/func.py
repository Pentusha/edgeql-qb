from collections.abc import Callable, Iterable

from edgeql_qb.expression import AnyExpression, Expression
from edgeql_qb.func import Function
from edgeql_qb.render.tools import (
    all_to_infix,
    do,
    join_with,
    render_parentheses,
)
from edgeql_qb.render.types import RenderedQuery


def render_function_args(args: list[RenderedQuery]) -> RenderedQuery:
    return (
        (args @ join_with(', ') if args else RenderedQuery())
        @ do(render_parentheses)
    )


def render_function(func: Function, arg_renderers: list[RenderedQuery]) -> RenderedQuery:
    prefix = f'{func.module}::{func.name}' if func.module != 'std' else func.name
    return render_function_args(arg_renderers).with_prefix(prefix)


def render_generic_function(
        inp: Iterable[AnyExpression],
        closure: Callable[..., RenderedQuery],
        func: Function,
) -> RenderedQuery:
    return (
        inp
        @ do(map, Expression)
        @ all_to_infix
        @ do.each(closure)
        @ do(render_function, func)
    )
