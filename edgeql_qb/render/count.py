from collections.abc import Iterator

from edgeql_qb.expression import Expression
from edgeql_qb.render.condition import render_conditions
from edgeql_qb.render.select import render_select
from edgeql_qb.render.tools import combine_many_renderers
from edgeql_qb.render.types import RenderedQuery


def render_count(inner: RenderedQuery) -> RenderedQuery:
    return combine_many_renderers(
        RenderedQuery('select count('),
        inner,
        RenderedQuery(')'),
    )

def render_count_inner(
        model_name: str,
        filters: tuple[Expression, ...],
        gen: Iterator[int],
) -> RenderedQuery:
    match filters:
        case ():
            return RenderedQuery(model_name)
        case conditions:
            rendered_select = render_select(
                model_name,
                select=(),
                generator=gen,
            )
            rendered_filters = render_conditions(conditions, gen)
            return combine_many_renderers(
                RenderedQuery('('),
                rendered_select,
                rendered_filters,
                RenderedQuery(')'),
            )
