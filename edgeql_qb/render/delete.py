from edgeql_qb.render.types import RenderedQuery


def render_delete(model_name: str) -> RenderedQuery:
    return RenderedQuery(model_name).with_prefix('delete ')
