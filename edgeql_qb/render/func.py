from functools import reduce

from edgeql_qb.render.tools import join_renderers, render_parentheses
from edgeql_qb.render.types import RenderedQuery


def render_function_args(args: list[RenderedQuery]) -> RenderedQuery:
    return render_parentheses(reduce(join_renderers(', '), args))
