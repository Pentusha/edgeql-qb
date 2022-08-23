from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from functools import singledispatch
from typing import TYPE_CHECKING, Any, Generic, Iterator, TypeVar, cast

from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import (
    Alias,
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
from edgeql_qb.types import unsafe_text

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
    alias = auto()
    text = auto()
    func_invocation = auto()


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
    | FuncInvocation
    | unsafe_text
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
def _(value: Alias) -> SymbolType:
    return SymbolType.alias


@_determine_type.register
def _(value: unsafe_text) -> SymbolType:
    return SymbolType.text


@_determine_type.register
def _(value: FuncInvocation) -> SymbolType:
    return SymbolType.func_invocation


@_determine_type.register
def _(value: str) -> SymbolType:
    if value in sort_ops:
        return SymbolType.sort_direction
    elif value in Ops:
        return SymbolType.operator
    else:
        return SymbolType.literal


class Symbol:
    __slots__ = 'value', 'type', 'arity', 'depth'

    def __init__(self, value: Any, arity: int | None = None, depth: int = 0):
        self.value = value
        self.type = _determine_type(value)
        self.arity = arity
        self.depth = depth


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
        case SymbolType.column | SymbolType.subselect | SymbolType.text | SymbolType.alias:
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
        case SymbolType.func_invocation:
            args = stack.popn(symbol.value.arity)
            invocation = FuncInvocation(
                func=symbol.value.func,
                args=tuple(args),
                arity=symbol.value.arity,
            )
            stack.push(invocation)
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
            depth: int = 0,
    ) -> Iterator[Symbol]:
        match expr:
            case Column() | Alias():
                yield Symbol(expr, depth=depth + 1)
            case UnaryOp(operation, element):
                yield Symbol(operation, arity=1, depth=depth + 1)
                yield from self._to_polish_notation(element, depth + 1)
            case BinaryOp(operation, left, right):
                yield Symbol(operation, arity=2, depth=depth + 1)

                # replace nested alias assignments with corresponding symbol
                # a := (b := value) + 1 -> a := b + 1
                #   ^ 0   ^ 1       ^ 2 depth
                if isinstance(left, BinaryOp) and left.operation == ':=' and depth > 0:
                    left = left.left
                # a := 1 + (b := value) -> a := 1 + b
                #   ^ 0  ^ 2  ^ 1 depth
                if isinstance(right, BinaryOp) and right.operation == ':=' and depth > 0:
                    right = right.left

                yield from self._to_polish_notation(left, depth + 1)
                yield from self._to_polish_notation(right, depth + 1)
            case SortedExpression(expression, direction):
                yield Symbol(direction, depth=depth)
                yield from self._to_polish_notation(expression, depth + 1)
            case FuncInvocation(_, args):
                yield Symbol(expr, arity=expr.arity, depth=depth)
                for arg in args:
                    if isinstance(arg, BinaryOp) and arg.operation == ':=':
                        arg = arg.left
                    yield from self._to_polish_notation(arg, depth + 1)
            case _:
                yield Symbol(expr, depth=depth)
