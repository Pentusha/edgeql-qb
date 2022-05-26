from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from functools import reduce
from typing import Any, Callable, cast

from edgeql_qb.expression import (
    AnyExpression,
    Expression,
    QueryLiteral,
    SelectExpressions,
)
from edgeql_qb.operators import (
    BinaryOp,
    Column,
    Columns,
    Node,
    SortedExpression,
    SubSelect,
    UnaryOp,
)
from edgeql_qb.types import GenericHolder


@dataclass
class EdgeDBModel:
    name: str
    c: Columns = field(default_factory=Columns)

    def select(self, *selectables: SelectExpressions) -> 'Query':
        return Query(self, select=[Expression(sel) for sel in selectables])


@dataclass
class RenderedQuery:
    query: str = ''
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class Query:
    model: EdgeDBModel
    select: list[Expression] = field(default_factory=list)
    filters: list[Expression] = field(default_factory=list)
    ordered_by: list[Expression] = field(default_factory=list)
    limit_val: int | None = None
    offset_val: int | None = None

    def where(self, compared: BinaryOp | UnaryOp) -> 'Query':
        self.filters.append(Expression(compared))
        return self

    def order_by(self, *columns: SortedExpression | Column | UnaryOp) -> 'Query':
        self.ordered_by = [Expression(exp) for exp in columns]
        return self

    def limit(self, value: int) -> 'Query':
        self.limit_val = value
        return self

    def offset(self, value: int) -> 'Query':
        self.offset_val = value
        return self

    def all(self) -> RenderedQuery:
        return Renderer().render(self)


@dataclass
class Renderer:
    # literal placeholder prefixes
    filter_prefix = 'filter'
    select_prefix = 'select'
    order_by_prefix = 'order_by'

    @classmethod
    def linearize_filter_left(cls, column: Column) -> list[Column]:
        match column.parent:
            case None:
                return [column]
            case _:
                return [*cls.linearize_filter_left(column.parent), column]

    def render_query_literal(self, name: str, value: Any) -> RenderedQuery:  # noqa: C901
        if isinstance(value, GenericHolder):
            return RenderedQuery(f'<{value.edgeql_name}>${name}', {name: value.value})
        elif (
            isinstance(value, (str, bool, bytes))
            or (isinstance(value, datetime) and value.tzinfo is not None)
        ):
            return RenderedQuery(f'<{value.__class__.__name__}>${name}', {name: value})
        elif isinstance(value, datetime):
            return RenderedQuery(f'<cal::local_datetime>${name}', {name: value})
        elif isinstance(value, date):
            return RenderedQuery(f'<cal::local_date>${name}', {name: value})
        elif isinstance(value, time):
            return RenderedQuery(f'<cal::local_time>${name}', {name: value})
        elif isinstance(value, timedelta):
            return RenderedQuery(f'<duration>${name}', {name: value})
        elif isinstance(value, Decimal):
            return RenderedQuery(f'<decimal>${name}', {name: value})
        else:
            return RenderedQuery(f'${name}', {name: value})  # pragma: no cover

    def render_right_parentheses(
        self,
        right: Node,
        expression: Node,
        right_column: RenderedQuery,
    ) -> RenderedQuery:
        right_expr_parenthesis = (
            isinstance(right, Node)
            and right < expression
            or (right == expression and expression.assocright)
            or (getattr(right, 'op', None) == '-' and expression.assocright)
        )
        if right_expr_parenthesis:
            right_column = combine_many_renderers([
                RenderedQuery('('),
                right_column,
                RenderedQuery(')'),
            ])
        return right_column

    def render_left_parentheses(
        self,
        left: Node,
        expression: Node,
        left_column: RenderedQuery,
    ) -> RenderedQuery:
        left_expr_parenthesis = (
            isinstance(left, Node)
            and left < expression
            or (
                left == expression
                and not expression.assocright
                and (
                    not isinstance(left, Column)
                    and getattr(left, 'operation', 1) != getattr(expression, 'operation', 2)
                )
            )
        )
        if left_expr_parenthesis:
            left_column = combine_many_renderers([
                RenderedQuery('('),
                left_column,
                RenderedQuery(')'),
            ])
        return left_column

    def render_filter(self, expression: AnyExpression, index: int) -> RenderedQuery:
        match expression:
            case Node(left, op, None):  # unary
                return combine_many_renderers([
                    RenderedQuery(op),
                    self.render_filter(left, index),
                ])
            case Node(left, op, right):
                right_column = self.render_filter(cast(Node, right), index)
                right_column = self.render_right_parentheses(
                    cast(Node, right),
                    expression,
                    right_column
                )
                left_column = self.render_filter(expression.left, index)
                left_column = self.render_left_parentheses(left, expression, left_column)
                return combine_many_renderers([
                    left_column,
                    RenderedQuery(f' {op} '),
                    right_column,
                ])
            case QueryLiteral(value, node_index):
                name = f'{self.filter_prefix}_{index}_{node_index}'
                return self.render_query_literal(name, value)
            case Column(name):
                columns = self.linearize_filter_left(expression)
                dot_names = '.'.join(c.column_name for c in columns)
                return RenderedQuery(f'.{dot_names}')
            case _:  # pragma: no cover
                assert False

    def render_select_expression(
        self,
        expression: AnyExpression,
        index: int,
        column_prefix: str = '',
    ) -> RenderedQuery:
        match expression:
            case Column(name):
                return RenderedQuery(f'{column_prefix}{name}')
            case SubSelect(parent, columns):
                expressions = [
                    self.render_select_expression(exp, index, column_prefix)
                    for exp in columns
                ]
                return combine_many_renderers([
                    RenderedQuery(f'{parent.column_name}: {{ '),
                    reduce(join_renderers(', '), expressions),
                    RenderedQuery(' }')
                ])
            case QueryLiteral(value, node_index):
                name = f'{self.select_prefix}_{index}_{node_index}'
                return self.render_query_literal(name, value)
            case Node(left, op, None):  # unary
                return combine_many_renderers([
                    RenderedQuery(op),
                    self.render_select_expression(left, index, column_prefix),
                ])
            case Node(left, op, right):
                right_column = self.render_select_expression(
                    cast(Node, right),
                    index,
                    column_prefix='.',
                )
                right_column = self.render_right_parentheses(
                    cast(Node, right),
                    expression,
                    right_column,
                )
                left_column = self.render_select_expression(
                    left,
                    index,
                    column_prefix=op != ':=' and '.' or '',
                )
                left_column = self.render_left_parentheses(left, expression, left_column)
                return combine_many_renderers([
                    left_column,
                    RenderedQuery(f' {op} '),
                    right_column,
                ])
            case _:  # pragma: no cover
                assert False

    def render_order_by_expression(
            self,
            expression: AnyExpression,
            index: int,
    ) -> RenderedQuery:
        match expression:
            case Column(name):
                return RenderedQuery(f'.{name}')
            case Node(left, op, None):  # unary
                return combine_many_renderers([
                    RenderedQuery(op),
                    self.render_order_by_expression(left, index),
                ])
            case Node(left, op, right):
                right_column = self.render_order_by_expression(cast(Node, right), index)
                right_column = self.render_right_parentheses(
                    cast(Node, right),
                    expression,
                    right_column,
                )
                left_column = self.render_order_by_expression(left, index)
                left_column = self.render_left_parentheses(left, expression, left_column)
                return combine_many_renderers([
                    left_column,
                    RenderedQuery(f' {op} '),
                    right_column,
                ])
            case SortedExpression(expr, order):
                return combine_renderers(
                    self.render_order_by_expression(expr, index),
                    RenderedQuery(f' {order}'),
                )
            case QueryLiteral(value, node_index):
                name = f'{self.order_by_prefix}_{index}_{node_index}'
                return self.render_query_literal(name, value)
            case _:  # pragma: no cover
                assert False

    def render_select(
            self,
            model_name: str,
            select: list[Expression],
    ) -> RenderedQuery:
        renderers = [
            self.render_select_expression(selectable.to_infix_notation(), index)
            for index, selectable in enumerate(select)
        ]
        return combine_many_renderers([
            RenderedQuery(f'select {model_name} {{ '),
            reduce(join_renderers(', '), renderers),
            RenderedQuery(' }'),
        ])

    def render_filters(self, filters: list[Expression]) -> RenderedQuery:
        if filters:
            renderers = [
                self.render_filter(filter_.to_infix_notation(), index)
                for index, filter_ in enumerate(filters)
            ]
            return combine_renderers(
                RenderedQuery(' filter '),
                reduce(join_renderers(' and '), renderers),
            )
        else:
            return RenderedQuery()

    def render_order_by(self, ordered_by: list[Expression]) -> RenderedQuery:
        if ordered_by:
            renderers = [
                self.render_order_by_expression(expression.to_infix_notation(), index)
                for index, expression in enumerate(ordered_by)
            ]
            return combine_renderers(
                RenderedQuery(' order by '),
                reduce(join_renderers(' then '), renderers)
            )
        else:
            return RenderedQuery()

    def render_limit(self, limit: int | None) -> RenderedQuery:
        if limit is not None:
            return RenderedQuery(' limit <int64>$limit', {'limit': limit})
        return RenderedQuery()

    def render_offset(self, offset: int | None) -> RenderedQuery:
        if offset is not None:
            return RenderedQuery(' offset <int64>$offset', {'offset': offset})
        return RenderedQuery()

    def render(self, query: Query) -> RenderedQuery:
        rendered_select = self.render_select(query.model.name, query.select)
        rendered_filters = self.render_filters(query.filters)
        rendered_order_by = self.render_order_by(query.ordered_by)
        rendered_offset = self.render_offset(query.offset_val)
        rendered_limit = self.render_limit(query.limit_val)
        return combine_many_renderers([
            rendered_select,
            rendered_filters,
            rendered_order_by,
            rendered_offset,
            rendered_limit,
        ])


def combine_many_renderers(renderers: list[RenderedQuery]) -> RenderedQuery:
    return reduce(combine_renderers, renderers)


def join_renderers(separator: str = '') -> Callable[[RenderedQuery, RenderedQuery], RenderedQuery]:
    def inner(r1: RenderedQuery, r2: RenderedQuery) -> RenderedQuery:
        return RenderedQuery(
            query=f'{r1.query}{separator}{r2.query}',
            context=r1.context | r2.context,
        )
    return inner


def combine_renderers(r1: RenderedQuery, r2: RenderedQuery) -> RenderedQuery:
    return join_renderers()(r1, r2)
