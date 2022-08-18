from dataclasses import dataclass, field, replace
from typing import Any

from edgeql_qb.expression import Expression, SelectExpressions, SubQuery
from edgeql_qb.operators import (
    BinaryOp,
    Column,
    Columns,
    SortedExpression,
    UnaryOp,
)
from edgeql_qb.render.condition import render_conditions
from edgeql_qb.render.delete import render_delete
from edgeql_qb.render.insert import render_insert
from edgeql_qb.render.insert import render_values as render_insert_values
from edgeql_qb.render.order_by import render_order_by
from edgeql_qb.render.select import render_limit, render_offset, render_select
from edgeql_qb.render.tools import combine_many_renderers
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.render.update import render_update
from edgeql_qb.render.update import render_values as render_update_values
from edgeql_qb.types import unsafe_text


@dataclass(slots=True, frozen=True)
class EdgeDBModel:
    name: str
    module: str | None = None
    schema: str = 'default'
    c: Columns = field(default_factory=Columns)

    def select(self, *selectables: SelectExpressions) -> 'SelectQuery':
        return SelectQuery(self, select=tuple(Expression(sel) for sel in selectables))

    @property
    def delete(self) -> 'DeleteQuery':
        return DeleteQuery(self)

    @property
    def insert(self) -> 'InsertQuery':
        return InsertQuery(self)

    @property
    def update(self) -> 'UpdateQuery':
        return UpdateQuery(self)


@dataclass(slots=True, frozen=True)
class SelectQuery(SubQuery):
    model: EdgeDBModel
    select: tuple[Expression, ...] = field(default_factory=tuple)
    filters: tuple[Expression, ...] = field(default_factory=tuple)
    ordered_by: tuple[Expression, ...] = field(default_factory=tuple)
    limit_val: int | unsafe_text | None = None
    offset_val: int | unsafe_text | None = None

    def where(self, compared: BinaryOp | UnaryOp) -> 'SelectQuery':
        return replace(self, filters=(*self.filters, Expression(compared)))

    def order_by(self, *columns: SortedExpression | Column | UnaryOp) -> 'SelectQuery':
        new_expressions = [Expression(exp) for exp in columns]
        return replace(self, ordered_by=(*self.ordered_by, *new_expressions))

    def limit(self, value: int | unsafe_text) -> 'SelectQuery':
        return replace(self, limit_val=value)

    def offset(self, value: int | unsafe_text) -> 'SelectQuery':
        return replace(self, offset_val=value)

    def all(self, query_index: int = 0) -> RenderedQuery:
        rendered_select = render_select(self.model.name, self.select, self.model.module)
        rendered_filters = render_conditions(self.filters, query_index)
        rendered_order_by = render_order_by(self.ordered_by, query_index)
        rendered_offset = render_offset(self.offset_val, query_index)
        rendered_limit = render_limit(self.limit_val, query_index)
        return combine_many_renderers(
            rendered_select,
            rendered_filters,
            rendered_order_by,
            rendered_offset,
            rendered_limit,
        )


@dataclass(slots=True, frozen=True)
class DeleteQuery:
    model: EdgeDBModel
    filters: tuple[Expression, ...] = field(default_factory=tuple)

    def where(self, compared: BinaryOp | UnaryOp) -> 'DeleteQuery':
        return replace(self, filters=(*self.filters, Expression(compared)))

    def all(self, query_index: int = 0) -> RenderedQuery:
        rendered_delete = render_delete(self.model.name)
        rendered_filters = render_conditions(self.filters, query_index)
        return combine_many_renderers(rendered_delete, rendered_filters)


@dataclass(slots=True, frozen=True)
class InsertQuery(SubQuery):
    model: EdgeDBModel
    values_to_insert: list[Expression] = field(default_factory=list)

    def values(self, **to_insert: Any) -> 'InsertQuery':
        assert to_insert
        values_to_insert = [
            Expression(BinaryOp(':=', Column(name), exp))
            for name, exp in to_insert.items()
        ]
        return replace(self, values_to_insert=values_to_insert)

    def all(self, query_index: int = 0) -> RenderedQuery:
        assert self.values_to_insert
        rendered_insert = render_insert(self.model.name)
        rendered_values = render_insert_values(self.values_to_insert, query_index)
        return combine_many_renderers(rendered_insert, rendered_values)


@dataclass(slots=True, frozen=True)
class UpdateQuery:
    model: EdgeDBModel
    values_to_update: tuple[Expression, ...] = field(default_factory=tuple)
    filters: tuple[Expression, ...] = field(default_factory=tuple)

    def where(self, compared: BinaryOp | UnaryOp) -> 'UpdateQuery':
        return replace(self, filters=(*self.filters, Expression(compared)))

    def values(self, **to_update: Any) -> 'UpdateQuery':
        assert to_update
        values_to_update = tuple(
            Expression(BinaryOp(':=', Column(name), exp))
            for name, exp in to_update.items()
        )
        return replace(self, values_to_update=values_to_update)

    def all(self, query_index: int = 0) -> RenderedQuery:
        assert self.values_to_update
        rendered_insert = render_update(self.model.name)
        rendered_filters = render_conditions(self.filters, query_index)
        rendered_values = render_update_values(self.values_to_update, query_index)
        return combine_many_renderers(rendered_insert, rendered_filters, rendered_values)
