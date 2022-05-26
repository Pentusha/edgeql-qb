from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Generic, Iterator, TypeVar, cast

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
    prec_dict,
    sort_ops,
)


@dataclass(unsafe_hash=True)
class QueryLiteral:
    value: Any
    index: int


class SymbolType(Enum):
    column = auto()
    subselect = auto()
    literal = auto()
    operator = auto()


SelectExpressions = (
    Column
    | SubSelect
    | BinaryOp
    | Node
    | QueryLiteral
)
AnyExpression = (
    Column
    | SubSelect
    | BinaryOp
    | Node
    | QueryLiteral
    | UnaryOp
    | SortedExpression
    | OperationsMixin
)
FilterExpressions = BinaryOp | UnaryOp


class Symbol:
    __slots__ = 'value', 'type', 'arity'

    def __init__(self, value: Any, arity: int | None = None):
        self.value = value
        self.type = self._determine_type(value)
        self.arity = arity

    def _determine_type(self, value: Any) -> SymbolType:
        if isinstance(value, Column):
            return SymbolType.column
        if value in Ops:
            return SymbolType.operator
        return SymbolType.literal

    def __iter__(self) -> Iterator[Any]:
        yield from (self.type, self.arity, self.value)


def build_binary_op(op: OpLiterals, left: Node, right: Node) -> Node:
    if (
        op == getattr(right, 'op', None) == '-'
        and right.right is None
    ):
        # a - -b = a + b
        return Node(left, '+', right.left)

    elif (
        op == '-'
        and getattr(right, 'op', None) == '+'
        and right.right is None
    ):
        # a - +b = a - b
        return Node(left, '-', right.left)

    elif (
        op == '+'
        and getattr(right, 'op', None) == '-'
        and right.right is None
    ):
        # a + -b = a - b
        return Node(
            left,
            '-',
            right.left,
        )
    else:
        return Node(left, op, right)


def build_unary_op(op: OpLiterals, argument: Node) -> Node:
    return Node(argument, op)


class Expression:
    def __init__(self, expression: AnyExpression):
        self.serialized = tuple(self._to_polish_notation(expression))

    def to_infix_notation(self) -> 'AnyExpression':
        stack = Stack[AnyExpression]()
        for index, (itype, arity, value) in enumerate(reversed(self.serialized)):
            if isinstance(value, (Column, SubSelect)):
                stack.push(value)
            elif value in sort_ops:
                sorted_expression = stack.pop()
                stack.push(SortedExpression(cast(OperationsMixin, sorted_expression), value))
            elif value in prec_dict and arity:
                arguments = stack.popn(arity)
                if arity == 1:
                    argument, = arguments
                    stack.push(build_unary_op(value, cast(Node, argument)))
                elif arity == 2:
                    left, right = arguments
                    stack.push(build_binary_op(value, cast(Node, left), cast(Node, right)))
                else:  # pragma: no cover
                    assert False
            else:
                stack.push(QueryLiteral(value, index))
        return stack.pop()

    def _to_polish_notation(
            self,
            expr: AnyExpression,
    ) -> Iterator[Symbol]:
        if isinstance(expr, Column):
            yield Symbol(expr)
        elif isinstance(expr, UnaryOp):
            target = expr.element
            yield Symbol(expr.operation, arity=1)
            yield from self._to_polish_notation(target)
        elif isinstance(expr, BinaryOp):
            yield Symbol(expr.operation, arity=2)
            yield from self._to_polish_notation(expr.left)
            yield from self._to_polish_notation(expr.right)
        elif isinstance(expr, SortedExpression):
            yield Symbol(expr.order)
            yield from self._to_polish_notation(expr.expression)
        else:
            yield Symbol(expr)


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
