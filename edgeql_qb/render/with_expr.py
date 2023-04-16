from collections.abc import Callable, Iterator

from edgeql_qb.expression import Expression
from edgeql_qb.render.expression import render_expression
from edgeql_qb.render.tools import all_to_infix, do, join_with
from edgeql_qb.render.types import RenderedQuery


def render_with_expressions(
        expressions: list[RenderedQuery],
) -> Callable[[RenderedQuery], RenderedQuery]:
    def inner(rendered_with: RenderedQuery) -> RenderedQuery:
        return (
            expressions
            and rendered_with + expressions @ join_with(', ')
            or rendered_with
        )
    return inner


def render_with_module(module: str | None = None) -> Callable[[RenderedQuery], RenderedQuery]:
    def inner(rendered_with: RenderedQuery) -> RenderedQuery:
        return (
            module
            and rendered_with.with_postfix(f'module {module}')
            or rendered_with
        )
    return inner


def render_with_separator(
    expressions: list[RenderedQuery],
    module: str | None = None,
) -> Callable[[RenderedQuery], RenderedQuery]:
    """Put comma between module and expressions if both are presented."""
    def inner(rendered_with: RenderedQuery) -> RenderedQuery:
        return (
            (module and expressions)
            and rendered_with.with_postfix(', ')
            or rendered_with
        )
    return inner


def render_with_ending(
    expressions: list[RenderedQuery],
    module: str | None = None,
) -> Callable[[RenderedQuery], RenderedQuery]:
    """Put space on the end of expressions if any module or expressions presented."""
    def inner(rendered_with: RenderedQuery) -> RenderedQuery:
        space = RenderedQuery(' ')
        empty = RenderedQuery()
        return (
            (module or expressions)
            and rendered_with.with_postfix(' ')
            or ((not module and not expressions) and empty or space)
        )
    return inner


def render_with_expression(
        with_aliases: tuple[Expression, ...],
        generator: Iterator[int],
        module: str | None = None,
) -> RenderedQuery:
    closure = do(render_expression, literal_prefix='with', generator=generator)
    renderers: list[RenderedQuery] = (
        with_aliases
        @ all_to_infix
        @ do.each(closure)
        @ do(list[RenderedQuery])
    )
    return (
        RenderedQuery('with ')
        .map(render_with_module(module))
        .map(render_with_separator(renderers, module))
        .map(render_with_expressions(renderers))
        .map(render_with_ending(renderers, module))
    )
