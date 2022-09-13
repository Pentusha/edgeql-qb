from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from functools import singledispatch
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterator,
    Optional,
    TypeVar,
    Union,
    cast,
)

from edgeql_qb.func import FuncInvocation
from edgeql_qb.operators import (
    Alias,
    BinaryOp,
    Node,
    OperationsMixin,
    OpLiterals,
    Ops,
    SortedExpression,
    UnaryOp,
    sort_ops,
)
from edgeql_qb.types import unsafe_text

if TYPE_CHECKING:
    from edgeql_qb.render.types import RenderedQuery  # pragma: no cover


@dataclass(slots=True, frozen=True)
class QueryLiteral:
    value: Any


class SymbolType(Enum):
    column = auto()
    shape = auto()
    sort_direction = auto()
    literal = auto()
    operator = auto()
    subquery = auto()
    alias = auto()
    text = auto()
    func_invocation = auto()


@dataclass(slots=True, frozen=True)
class Shape:
    parent: 'Column'
    columns: tuple[Union['Column', 'Shape'], ...]
    filters: tuple['Expression', ...] = field(default_factory=tuple)

    def where(self, compared: Union['BinaryOp', 'UnaryOp', 'FuncInvocation']) -> 'Shape':
        return replace(self, filters=(*self.filters, Expression(compared)))


@dataclass(slots=True, frozen=True)
class Column(OperationsMixin):
    column_name: str
    parent: Optional['Column'] = None

    def __call__(self, *columns: Union['Column', Shape]) -> Shape:
        return Shape(self, columns)

    def __getattr__(self, name: str) -> 'Column':
        return Column(name, self)

    def __eq__(self, other: Any) -> BinaryOp:  # type: ignore
        return BinaryOp('=', self, other)

    def __ne__(self, other: Any) -> BinaryOp:  # type: ignore
        return BinaryOp('!=', self, other)


SelectExpressions = (
    Column
    | Shape
    | BinaryOp
    | Node
    | QueryLiteral
)


class SubQuery(ABC):
    @abstractmethod
    def all(self, generator: Iterator[int] | None = None) -> 'RenderedQuery':
        raise NotImplementedError()  # pragma: no cover


AnyExpression = (
    Column
    | Shape
    | BinaryOp
    | Node
    | QueryLiteral
    | UnaryOp
    | SortedExpression
    | OperationsMixin
    | SubQuery
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
def _(value: Shape) -> SymbolType:
    return SymbolType.shape


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


def evaluate(stack: Stack[AnyExpression], symbol: Symbol) -> None:
    match symbol.type:
        case SymbolType.column | SymbolType.shape | SymbolType.text | SymbolType.alias:
            stack.push(symbol.value)
        case SymbolType.subquery:
            stack.push(symbol.value)
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
            stack.push(QueryLiteral(symbol.value))
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


def _replace_alias_with_label(node: Any, depth: int) -> Any:
    """Replace assignment operation with label.
    Top level expression should not be replaced, so depth checking is necessary as well.
    Node(':=', left=Alias('test'), right=1) -> Alias('test')
    """
    match node:
        case BinaryOp(':=', left, _) if depth > 0:
            return left
        case _:
            return node


class Expression:
    def __init__(self, expression: AnyExpression):
        self.serialized = tuple(self._to_polish_notation(expression))

    def to_infix_notation(self) -> 'AnyExpression':
        stack = Stack[AnyExpression]()
        for symbol in reversed(self.serialized):
            evaluate(stack, symbol)
        return stack.pop()

    def _to_polish_notation(
            self,
            expr: AnyExpression,
            depth: int = 0,
    ) -> Iterator[Symbol]:
        match expr:
            case Column() | Alias():
                yield Symbol(expr, depth=depth)
            case UnaryOp(operation, element):
                yield Symbol(operation, arity=1, depth=depth)
                # a := -(b := 1) -> a := -b
                element = _replace_alias_with_label(element, depth)
                yield from self._to_polish_notation(element, depth + 1)
            case BinaryOp(operation, left, right):
                yield Symbol(operation, arity=2, depth=depth)

                # a := (b := value) + 1 -> a := b + 1
                left = _replace_alias_with_label(left, depth)
                # a := 1 + (b := value) -> a := 1 + b
                right = _replace_alias_with_label(right, depth)

                yield from self._to_polish_notation(left, depth + 1)
                yield from self._to_polish_notation(right, depth + 1)
            case SortedExpression(expression, direction):
                yield Symbol(direction, depth=depth)
                yield from self._to_polish_notation(expression, depth + 1)
            case FuncInvocation(_, args):
                yield Symbol(expr, arity=expr.arity, depth=depth)
                for arg in args:
                    # a := fun(b := 1, c := 2) -> a := fun(b, c)
                    arg = _replace_alias_with_label(arg, depth)
                    yield from self._to_polish_notation(arg, depth + 1)
            case _:
                yield Symbol(expr, depth=depth)


class Columns:
    def __getattribute__(self, name: str) -> Column:
        return Column(name)
