import pytest
from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.func import math
from edgeql_qb.operators import BinaryOp
from edgeql_qb.types import int16, int32, int64

A = EdgeDBModel('A')


def test_select_operators(client: Client) -> None:
    rendered = A.select(
        (A.c.p_bool & True).label('and_result'),
        (A.c.p_bool | True).label('or_result'),
        (A.c.p_int32 / A.c.p_int64).label('true_div_result'),
        (A.c.p_int32 // A.c.p_int64).label('floor_div_result'),
        (A.c.p_int32 % A.c.p_int64).label('mod_result'),
        (A.c.p_int32.coalesce(A.c.p_int64).label('coalesce_result')),
        (A.c.p_int32.in_(A.c.p_int32).label('in_result')),
        (A.c.p_int32.not_in(A.c.p_int16).label('not_in_result')),
        (math.abs(A.c.p_int32 - int32(20)) + int32(1)).label('abs'),
    ).build()
    insert = A.insert.values(p_bool=True, p_int32=int32(10)).build()
    client.query(insert.query, **insert.context)
    assert rendered.query == (
        'select A { '
        'and_result := .p_bool and <bool>$select_0, '
        'or_result := .p_bool or <bool>$select_1, '
        'true_div_result := .p_int32 / .p_int64, '
        'floor_div_result := .p_int32 // .p_int64, '
        'mod_result := .p_int32 % .p_int64, '
        'coalesce_result := .p_int32 ?? .p_int64, '
        'in_result := .p_int32 in .p_int32, '
        'not_in_result := .p_int32 not in .p_int16, '
        'abs := math::abs(.p_int32 - <int32>$select_2) + <int32>$select_3 '
        '}'
    )
    assert rendered.context == FrozenDict(
        select_0=True,
        select_1=True,
        select_2=20,
        select_3=1,
    )
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_select_concatenation(client: Client) -> None:
    rendered = A.select(A.c.p_str.concat(A.c.p_str).label('result')).build()
    rendered2 = A.select(A.c.p_str.op('++')(A.c.p_str).label('result')).build()
    insert = A.insert.values(p_str='Hello').build()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select A { result := .p_str ++ .p_str }'
    assert rendered.context == FrozenDict()

    assert rendered == rendered2

    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].result == 'HelloHello'


def test_select_like_ilike(client: Client) -> None:
    insert_lower = A.insert.values(p_str='hello').build()
    insert_upper = A.insert.values(p_str='HELLO').build()
    client.query(insert_lower.query, **insert_lower.context)
    client.query(insert_upper.query, **insert_upper.context)
    select = A.select(A.c.p_str).where(A.c.p_str.like('%ell%')).build()
    result = client.query(select.query, **select.context)
    assert len(result) == 1
    assert result[0].p_str == 'hello'

    select = A.select(A.c.p_str).where(A.c.p_str.ilike('%ell%')).build()
    result = client.query(select.query, **select.context)
    assert len(result) == 2


def test_select_unary_expression(client: Client) -> None:
    rendered = A.select((-A.c.p_int32).label('minus_p_int32')).build()
    insert = A.insert.values(p_int32=int32(5)).build()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select A { minus_p_int32 := -.p_int32 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].minus_p_int32 == -5


def test_select_left_parentheses(client: Client) -> None:
    insert = A.insert.values(p_int32=int32(5)).build()
    client.query(insert.query, **insert.context)
    rendered = A.select(((A.c.p_int32 + A.c.p_int32) * A.c.p_int32).label('result')).build()
    assert rendered.query == 'select A { result := (.p_int32 + .p_int32) * .p_int32 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].result == 50


@pytest.mark.parametrize(
    ['expression', 'expected_query'],
    (
        (A.c.p_int64 - -A.c.p_int64, 'select A { my_sum := .p_int64 + .p_int64 }'),
        (A.c.p_int64 + -A.c.p_int64, 'select A { my_sum := .p_int64 - .p_int64 }'),
        (A.c.p_int64 - +A.c.p_int64, 'select A { my_sum := .p_int64 - .p_int64 }'),
    ),
)
def test_select_binary_optimisations(expression: BinaryOp, expected_query: str) -> None:
    rendered = A.select(expression.label('my_sum')).build()
    assert rendered.query == expected_query


def test_select_expression(client: Client) -> None:
    insert = A.insert.values(p_int16=int16(16), p_int32=int32(4), p_int64=int64(11)).build()
    client.query(insert.query, **insert.context)
    rendered = A.select((A.c.p_int64 + A.c.p_int64).label('my_sum')).build()
    assert rendered.query == 'select A { my_sum := .p_int64 + .p_int64 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].my_sum == 22

    rendered = A.select((A.c.p_int16 + A.c.p_int32 == A.c.p_int64).label('is_equal')).build()
    assert rendered.query == 'select A { is_equal := .p_int16 + .p_int32 = .p_int64 }'


def test_select_expression_with_literal(client: Client) -> None:
    insert = A.insert.values(p_int64=int64(11)).build()
    client.query(insert.query, **insert.context)
    rendered = A.select((A.c.p_int64 + int64(1)).label('my_sum')).build()
    assert rendered.query == 'select A { my_sum := .p_int64 + <int64>$select_0 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].my_sum == 12
