from types import MappingProxyType
from typing import Any

import pytest
from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.func import FuncInvocation, std
from edgeql_qb.operators import BinaryOp
from edgeql_qb.types import int32, int64, unsafe_text

A = EdgeDBModel('A')
Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')


@pytest.mark.parametrize(
    ['condition', 'expected_condition', 'expected_context'],
    (
        (A.c.p_int32 < A.c.p_int64, '.p_int32 < .p_int64', {}),
        (A.c.p_int64 != int64(1), '.p_int64 != <int64>$filter_1_0_0', {'filter_1_0_0': 1}),
        (A.c.p_int64 >= int64(1), '.p_int64 >= <int64>$filter_1_0_0', {'filter_1_0_0': 1}),
        (A.c.p_int32 <= int32(1), '.p_int32 <= <int32>$filter_1_0_0', {'filter_1_0_0': 1}),
        (
            (A.c.p_int32 + A.c.p_int64) * int64(1) > int64(2),
            '(.p_int32 + .p_int64) * <int64>$filter_1_0_1 > <int64>$filter_1_0_0',
            {'filter_1_0_1': 1, 'filter_1_0_0': 2},
        ),
        (
            A.c.p_int32 * (A.c.p_int64 + int64(1)) > int64(2),
            '.p_int32 * (.p_int64 + <int64>$filter_1_0_1) > <int64>$filter_1_0_0',
            {'filter_1_0_1': 1, 'filter_1_0_0': 2},
        ),
        (A.c.p_str == unsafe_text("'Hello'"), ".p_str = 'Hello'", {}),
        (
            std.contains(A.c.p_str, 'He'),
            'contains(.p_str, <str>$filter_1_0_0)',
            {'filter_1_0_0': 'He'},
        ),
    ),
)
def test_complex_filter_with_literal(
    client: Client,
    condition: BinaryOp | FuncInvocation,
    expected_condition: str,
    expected_context: dict[str, Any],
) -> None:
    insert = A.insert.values(p_int64=int64(11), p_int32=int32(1), p_str='Hello').all()
    client.query(insert.query, **insert.context)
    rendered = A.select(A.c.p_int64).where(condition).all()
    assert rendered.query == f'select A {{ p_int64 }} filter {expected_condition}'
    assert rendered.context == MappingProxyType(expected_context)
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_filter_with_unary_op(client: Client) -> None:
    insert = A.insert.values(p_int64=int64(11), p_bool=False).all()
    client.query(insert.query, **insert.context)
    rendered = (
        A.select(A.c.p_int64)
        .where(~A.c.p_bool)
        .all()
    )
    assert rendered.query == 'select A { p_int64 } filter not .p_bool'
    assert rendered.context == MappingProxyType({})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].p_int64 == 11


def test_nested_filter(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
    ).where(Nested1.c.nested2.name == 'n2').all()
    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(name='n2'),
    ).all()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select Nested1 { name } filter .nested2.name = <str>$filter_1_0_0'
    assert rendered.context == MappingProxyType({'filter_1_0_0': 'n2'})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
