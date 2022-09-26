from dataclasses import dataclass, field, replace
from typing import Any, Iterator, Union

from edgeql_qb.expression import (
    BaseModel,
    Column,
    Columns,
    Expression,
    SelectExpressions,
    SubQuery,
    UnlessConflict,
    UpdateSubQuery,
)
from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import BinaryOp, SortedExpression, UnaryOp
from edgeql_qb.render.condition import render_conditions
from edgeql_qb.render.delete import render_delete
from edgeql_qb.render.group import (
    render_group,
    render_group_by_expressions,
    render_using_expressions,
)
from edgeql_qb.render.insert import render_insert, render_unless_conflict
from edgeql_qb.render.insert import render_values as render_insert_values
from edgeql_qb.render.order_by import render_order_by
from edgeql_qb.render.pagination import render_limit, render_offset
from edgeql_qb.render.select import render_select
from edgeql_qb.render.tools import combine_many_renderers
from edgeql_qb.render.types import RenderedQuery
from edgeql_qb.render.update import render_update
from edgeql_qb.render.update import render_values as render_update_values
from edgeql_qb.render.with_expr import render_with_expression
from edgeql_qb.types import unsafe_text


def literal_index_generator(start: int = 0) -> Iterator[int]:
    index = start
    while True:
        yield index
        index += 1


@dataclass(slots=True, frozen=True)
class EdgeDBModel(BaseModel):
    c: Columns = field(default_factory=Columns)

    def select(self, *selectables: SelectExpressions, **kwargs: SubQuery) -> 'SelectQuery':
        select_args = tuple(Expression(sel) for sel in selectables)
        select_kwargs = tuple(Expression(v.label(k)) for k, v in kwargs.items())
        return SelectQuery(
            model=self,
            select=(*select_args, *select_kwargs),
        )

    def group(self, *selectables: SelectExpressions) -> 'GroupQuery':
        return GroupQuery(
            model=self,
            select=tuple(Expression(sel) for sel in selectables),
        )

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
    with_aliases: tuple[Expression, ...] = field(default_factory=tuple)
    filters: tuple[Expression, ...] = field(default_factory=tuple)
    ordered_by: tuple[Expression, ...] = field(default_factory=tuple)
    limit_val: int | unsafe_text | None = None
    offset_val: int | unsafe_text | None = None

    def where(self, compared: BinaryOp | UnaryOp | FuncInvocation) -> 'SelectQuery':
        return replace(self, filters=(*self.filters, Expression(compared)))

    def with_(self, *with_aliases: BinaryOp) -> 'SelectQuery':
        expressions = tuple(Expression(exp) for exp in with_aliases)
        return replace(self, with_aliases=expressions)

    def order_by(
        self,
        *columns: SortedExpression | Column | UnaryOp | FuncInvocation,
    ) -> 'SelectQuery':
        new_expressions = [Expression(exp) for exp in columns]
        return replace(self, ordered_by=(*self.ordered_by, *new_expressions))

    def limit(self, value: int | FuncInvocation | unsafe_text) -> 'SelectQuery':
        return replace(self, limit_val=value)

    @property
    def limit1(self) -> 'SelectQuery':
        return replace(self, limit_val=unsafe_text('1'))

    def offset(self, value: int | FuncInvocation | unsafe_text) -> 'SelectQuery':
        return replace(self, offset_val=value)

    def all(self, generator: Iterator[int] | None = None) -> RenderedQuery:
        gen = generator or literal_index_generator()
        rendered_with = render_with_expression(self.with_aliases, gen)
        rendered_select = render_select(
            self.model.name,
            self.select,
            gen,
            self.model.module,
        )
        rendered_filters = render_conditions(self.filters, gen)
        rendered_order_by = render_order_by(self.ordered_by, gen)
        rendered_offset = render_offset(self.offset_val, gen)
        rendered_limit = render_limit(self.limit_val, gen)
        return combine_many_renderers(
            rendered_with,
            rendered_select,
            rendered_filters,
            rendered_order_by,
            rendered_offset,
            rendered_limit,
        )


@dataclass(slots=True, frozen=True)
class GroupQuery:
    model: EdgeDBModel
    select: tuple[Expression, ...] = field(default_factory=tuple)
    with_aliases: tuple[Expression, ...] = field(default_factory=tuple)
    group_by: tuple[Column, ...] = field(default_factory=tuple)
    using_expressions: tuple[Expression, ...] = field(default_factory=tuple)

    def with_(self, *with_aliases: BinaryOp) -> 'GroupQuery':
        expressions = tuple(Expression(exp) for exp in with_aliases)
        return replace(self, with_aliases=expressions)

    def using(self, *using_expressions: BinaryOp) -> 'GroupQuery':
        expressions = tuple(Expression(exp) for exp in using_expressions)
        return replace(self, using_expressions=expressions)

    def by(self, *group_by: Column | BinaryOp) -> 'GroupQuery':
        return replace(self, group_by=group_by)

    def all(self, generator: Iterator[int] | None = None) -> RenderedQuery:
        gen = generator or literal_index_generator()
        rendered_with = render_with_expression(self.with_aliases, gen)
        rendered_group = render_group(self.model.name, self.select, gen)
        rendered_using = render_using_expressions(self.using_expressions, gen)
        rendered_group_by = render_group_by_expressions(self.group_by)
        return combine_many_renderers(
            rendered_with,
            rendered_group,
            rendered_using,
            rendered_group_by,
        )


@dataclass(slots=True, frozen=True)
class DeleteQuery:
    model: EdgeDBModel
    with_aliases: tuple[Expression, ...] = field(default_factory=tuple)
    filters: tuple[Expression, ...] = field(default_factory=tuple)
    ordered_by: tuple[Expression, ...] = field(default_factory=tuple)
    limit_val: int | unsafe_text | None = None
    offset_val: int | unsafe_text | None = None

    def where(self, compared: BinaryOp | UnaryOp) -> 'DeleteQuery':
        return replace(self, filters=(*self.filters, Expression(compared)))

    def with_(self, *with_aliases: BinaryOp) -> 'DeleteQuery':
        expressions = tuple(Expression(exp) for exp in with_aliases)
        return replace(self, with_aliases=expressions)

    def order_by(
        self,
        *columns: SortedExpression | Column | UnaryOp | FuncInvocation,
    ) -> 'DeleteQuery':
        new_expressions = [Expression(exp) for exp in columns]
        return replace(self, ordered_by=(*self.ordered_by, *new_expressions))

    def limit(self, value: int | FuncInvocation | unsafe_text) -> 'DeleteQuery':
        return replace(self, limit_val=value)

    def offset(self, value: int | FuncInvocation | unsafe_text) -> 'DeleteQuery':
        return replace(self, offset_val=value)

    def all(self, generator: Iterator[int] | None = None) -> RenderedQuery:
        gen = generator or literal_index_generator()
        rendered_with = render_with_expression(self.with_aliases, gen)
        rendered_delete = render_delete(self.model.name)
        rendered_filters = render_conditions(self.filters, gen)
        rendered_order_by = render_order_by(self.ordered_by, gen)
        rendered_offset = render_offset(self.offset_val, gen)
        rendered_limit = render_limit(self.limit_val, gen)
        return combine_many_renderers(
            rendered_with,
            rendered_delete,
            rendered_filters,
            rendered_order_by,
            rendered_offset,
            rendered_limit,
        )


@dataclass(slots=True, frozen=True)
class InsertQuery(SubQuery):
    model: EdgeDBModel
    with_aliases: tuple[Expression, ...] = field(default_factory=tuple)
    values_to_insert: list[Expression] = field(default_factory=list)
    unless_conflict_value: UnlessConflict | None = None

    def values(self, **to_insert: Any) -> 'InsertQuery':
        assert to_insert
        values_to_insert = [
            Expression(BinaryOp(':=', Column(name), exp))
            for name, exp in to_insert.items()
        ]
        return replace(self, values_to_insert=values_to_insert)

    def with_(self, *with_aliases: BinaryOp) -> 'InsertQuery':
        expressions = tuple(Expression(exp) for exp in with_aliases)
        return replace(self, with_aliases=expressions)

    def unless_conflict(
        self,
        on: tuple[Column, ...] | Column | None = None,
        else_: Union[UpdateSubQuery, EdgeDBModel, None] = None,
    ) -> 'InsertQuery':
        return replace(self, unless_conflict_value=UnlessConflict(on=on, else_=else_))

    def all(self, generator: Iterator[int] | None = None) -> RenderedQuery:
        assert self.values_to_insert
        gen = generator or literal_index_generator()
        rendered_with = render_with_expression(self.with_aliases, gen)
        rendered_insert = render_insert(self.model.name)
        rendered_values = render_insert_values(self.values_to_insert, gen)
        rendered_conflicts = render_unless_conflict(self.unless_conflict_value, gen)
        return combine_many_renderers(
            rendered_with,
            rendered_insert,
            rendered_values,
            rendered_conflicts,
        )


@dataclass(slots=True, frozen=True)
class UpdateQuery(UpdateSubQuery):
    model: EdgeDBModel
    with_aliases: tuple[Expression, ...] = field(default_factory=tuple)
    values_to_update: tuple[Expression, ...] = field(default_factory=tuple)
    filters: tuple[Expression, ...] = field(default_factory=tuple)

    def where(self, compared: BinaryOp | UnaryOp) -> 'UpdateQuery':
        return replace(self, filters=(*self.filters, Expression(compared)))

    def with_(self, *with_aliases: BinaryOp) -> 'UpdateQuery':
        expressions = tuple(Expression(exp) for exp in with_aliases)
        return replace(self, with_aliases=expressions)

    def values(self, **to_update: Any) -> 'UpdateQuery':
        assert to_update
        values_to_update = tuple(
            Expression(BinaryOp(':=', Column(name), exp))
            for name, exp in to_update.items()
        )
        return replace(self, values_to_update=values_to_update)

    def all(self, generator: Iterator[int] | None = None) -> RenderedQuery:
        assert self.values_to_update
        gen = generator or literal_index_generator()
        rendered_with = render_with_expression(self.with_aliases, gen)
        rendered_insert = render_update(self.model.name)
        rendered_filters = render_conditions(self.filters, gen)
        rendered_values = render_update_values(self.values_to_update, gen)
        return combine_many_renderers(
            rendered_with,
            rendered_insert,
            rendered_filters,
            rendered_values,
        )
