from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from functools import singledispatch
from typing import TYPE_CHECKING, Any, Generic, Iterator, TypeVar, cast

from edgeql_qb.operators import (
    BinaryOp,
    Column,
    Node,
    OperationsMixin,
    OpLiterals,
    Ops,
    SortedExpression,
    SubSelect,
    UnaryOp,
    sort_ops,
)
from edgeql_qb.types import text

if TYPE_CHECKING:
    from edgeql_qb.render.types import RenderedQuery  # pragma: no cover


@dataclass(slots=True, frozen=True)
class QueryLiteral:
    value: Any
    expression_index: int
    query_index: int


class SymbolType(Enum):
    column = auto()
    subselect = auto()
    sort_direction = auto()
    literal = auto()
    operator = auto()
    subquery = auto()
    text = auto()


SelectExpressions = (
    Column
    | SubSelect
    | BinaryOp
    | Node
    | QueryLiteral
)


class SubQuery(ABC):
    @abstractmethod
    def all(self, query_index: int = 0) -> 'RenderedQuery':
        raise NotImplementedError()  # pragma: no cover


@dataclass(slots=True, frozen=True)
class SubQueryExpression:
    subquery: SubQuery
    index: int


AnyExpression = (
    Column
    | SubSelect
    | BinaryOp
    | Node
    | QueryLiteral
    | UnaryOp
    | SortedExpression
    | OperationsMixin
    | SubQueryExpression
    | text
)
FilterExpressions = BinaryOp | UnaryOp
StackType = TypeVar('StackType')


class Stack(Generic[StackType]):
    def __init__(self) -> None:
        self._stack = deque[StackType]()

    def push(self, frame: StackType) -> None:
        self._stack.append(frame)

    def pop(self) -> StackType:
        return self._stack.pop()

    def popn(self, argcount: int) -> list[StackType]:
        return [self._stack.pop() for _ in range(argcount)]


@singledispatch
def _determine_type(value: Any) -> SymbolType:
    return SymbolType.literal


@_determine_type.register
def _(value: Column) -> SymbolType:
    return SymbolType.column


@_determine_type.register
def _(value: SubSelect) -> SymbolType:
    return SymbolType.subselect


@_determine_type.register
def _(value: SubQuery) -> SymbolType:
    return SymbolType.subquery


@_determine_type.register
def _(value: text) -> SymbolType:
    return SymbolType.text


@_determine_type.register
def _(value: str) -> SymbolType:
    if value in sort_ops:
        return SymbolType.sort_direction
    elif value in Ops:
        return SymbolType.operator
    else:
        return SymbolType.literal


class Symbol:
    __slots__ = 'value', 'type', 'arity'

    def __init__(self, value: Any, arity: int | None = None):
        self.value = value
        self.type = _determine_type(value)
        self.arity = arity


def build_binary_op(op: OpLiterals, left: Node, right: Node) -> Node:
    if op == getattr(right, 'op', None) == '-' and right.right is None:
        # a - -b = a + b
        return Node(left, '+', right.left)
    elif (
        (op == '+' and getattr(right, 'op', None) == '-')
        or (op == '-' and getattr(right, 'op', None) == '+')
        and right.right is None
    ):
        # a + -b = a - +b = a - b
        return Node(left, '-', right.left)
    else:
        return Node(left, op, right)


def build_unary_op(op: OpLiterals, argument: Node) -> Node:
    return Node(argument, op)


def evaluate(
    stack: Stack[AnyExpression],
    symbol: Symbol,
    query_index: int,
    expression_index: int,
) -> None:
    match symbol.type:
        case SymbolType.column | SymbolType.subselect | SymbolType.text:
            stack.push(symbol.value)
        case SymbolType.subquery:
            stack.push(SubQueryExpression(symbol.value, query_index))
        case SymbolType.operator:
            match symbol.arity:
                case 1:
                    argument = stack.pop()
                    stack.push(build_unary_op(symbol.value, cast(Node, argument)))
                case 2:
                    left, right = stack.popn(2)
                    stack.push(build_binary_op(symbol.value, cast(Node, left), cast(Node, right)))
                case _:  # pragma: no cover
                    assert False
        case SymbolType.literal:
            stack.push(QueryLiteral(symbol.value, expression_index, query_index))
        case SymbolType.sort_direction:
            sorted_expression = stack.pop()
            stack.push(SortedExpression(cast(OperationsMixin, sorted_expression), symbol.value))
        case _:  # pragma: no cover
            assert False


class Expression:
    def __init__(self, expression: AnyExpression):
        self.serialized = tuple(self._to_polish_notation(expression))

    def to_infix_notation(self, query_index: int = 0) -> 'AnyExpression':
        stack = Stack[AnyExpression]()
        for expression_index, symbol in enumerate(reversed(self.serialized)):
            evaluate(stack, symbol, query_index, expression_index)
        return stack.pop()

    def _to_polish_notation(
            self,
            expr: AnyExpression,
    ) -> Iterator[Symbol]:
        match expr:
            case Column():
                yield Symbol(expr)
            case UnaryOp(operation, element):
                yield Symbol(operation, arity=1)
                yield from self._to_polish_notation(element)
            case BinaryOp(operation, left, right):
                yield Symbol(operation, arity=2)
                yield from self._to_polish_notation(left)
                yield from self._to_polish_notation(right)
            case SortedExpression(expression, direction):
                yield Symbol(direction)
                yield from self._to_polish_notation(expression)
            case _:
                yield Symbol(expr)
