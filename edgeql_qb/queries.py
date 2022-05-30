from dataclasses import dataclass, field
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
from edgeql_qb.render.insert import render_insert, render_values
from edgeql_qb.render.order_by import render_order_by
from edgeql_qb.render.select import render_limit, render_offset, render_select
from edgeql_qb.render.tools import combine_many_renderers
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.types import text


@dataclass
class EdgeDBModel:
    name: str
    c: Columns = field(default_factory=Columns)

    def select(self, *selectables: SelectExpressions) -> 'SelectQuery':
        return SelectQuery(self, select=[Expression(sel) for sel in selectables])

    @property
    def delete(self) -> 'DeleteQuery':
        return DeleteQuery(self)

    @property
    def insert(self) -> 'InsertQuery':
        return InsertQuery(self)


@dataclass
class SelectQuery(SubQuery):
    model: EdgeDBModel
    select: list[Expression] = field(default_factory=list)
    filters: list[Expression] = field(default_factory=list)
    ordered_by: list[Expression] = field(default_factory=list)
    limit_val: int | text | None = None
    offset_val: int | text | None = None

    def where(self, compared: BinaryOp | UnaryOp) -> 'SelectQuery':
        self.filters.append(Expression(compared))
        return self

    def order_by(self, *columns: SortedExpression | Column | UnaryOp) -> 'SelectQuery':
        self.ordered_by = [Expression(exp) for exp in columns]
        return self

    def limit(self, value: int | text) -> 'SelectQuery':
        self.limit_val = value
        return self

    def offset(self, value: int | text) -> 'SelectQuery':
        self.offset_val = value
        return self

    def all(self, query_index: int = 0) -> RenderedQuery:
        rendered_select = render_select(self.model.name, self.select)
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


@dataclass
class DeleteQuery:
    model: EdgeDBModel
    filters: list[Expression] = field(default_factory=list)

    def where(self, compared: BinaryOp | UnaryOp) -> 'DeleteQuery':
        self.filters.append(Expression(compared))
        return self

    def all(self, query_index: int = 0) -> RenderedQuery:
        rendered_delete = render_delete(self.model.name)
        rendered_filters = render_conditions(self.filters, query_index)
        return combine_many_renderers(rendered_delete, rendered_filters)


@dataclass
class InsertQuery(SubQuery):
    model: EdgeDBModel
    values_to_insert: list[Expression] = field(default_factory=list)

    def values(self, **to_insert: Any) -> 'InsertQuery':
        assert to_insert
        self.values_to_insert = [
            Expression(BinaryOp(':=', Column(name), exp))
            for name, exp in to_insert.items()
        ]
        return self

    def all(self, query_index: int = 0) -> RenderedQuery:
        assert self.values_to_insert
        rendered_insert = render_insert(self.model.name)
        rendered_values = render_values(self.values_to_insert, query_index)
        return combine_many_renderers(rendered_insert, rendered_values)
