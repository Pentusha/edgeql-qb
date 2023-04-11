from datetime import datetime, timezone
from typing import Any

import pytest
from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
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
        (A.c.p_int64 != int64(1), '.p_int64 != <int64>$filter_0', {'filter_0': 1}),
        (A.c.p_int64 >= int64(1), '.p_int64 >= <int64>$filter_0', {'filter_0': 1}),
        (A.c.p_int32 <= int32(1), '.p_int32 <= <int32>$filter_0', {'filter_0': 1}),
        (
            (A.c.p_int32 + A.c.p_int64) * int64(1) > int64(2),
            '(.p_int32 + .p_int64) * <int64>$filter_0 > <int64>$filter_1',
            {'filter_0': 1, 'filter_1': 2},
        ),
        (
            A.c.p_int32 * (A.c.p_int64 + int64(1)) > int64(2),
            '.p_int32 * (.p_int64 + <int64>$filter_0) > <int64>$filter_1',
            {'filter_0': 1, 'filter_1': 2},
        ),
        (A.c.p_str == unsafe_text("'Hello'"), ".p_str = 'Hello'", {}),
        (
            std.contains(A.c.p_str, 'He'),
            'contains(.p_str, <str>$filter_0)',
            {'filter_0': 'He'},
        ),
        (
            std.datetime_current() > datetime(2000, 1, 1, tzinfo=timezone.utc),
            'datetime_current() > <datetime>$filter_0',
            {'filter_0': datetime(2000, 1, 1, tzinfo=timezone.utc)},
        ),
    ),
)
def test_complex_filter_with_literal(
    client: Client,
    condition: BinaryOp | FuncInvocation,
    expected_condition: str,
    expected_context: dict[str, Any],
) -> None:
    insert = A.insert.values(p_int64=int64(11), p_int32=int32(1), p_str='Hello').build()
    client.query(insert.query, **insert.context)
    rendered = A.select(A.c.p_int64).where(condition).build()
    assert rendered.query == f'select A {{ p_int64 }} filter {expected_condition}'
    assert rendered.context == FrozenDict(expected_context)
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_filter_with_unary_op(client: Client) -> None:
    insert = A.insert.values(p_int64=int64(11), p_bool=False).build()
    client.query(insert.query, **insert.context)
    rendered = (
        A.select(A.c.p_int64)
        .where(~A.c.p_bool)
        .build()
    )
    assert rendered.query == 'select A { p_int64 } filter not .p_bool'
    assert rendered.context == FrozenDict()
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].p_int64 == 11


def test_nested_filter(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
    ).where(Nested1.c.nested2.name == 'n2').build()
    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(name='n2'),
    ).build()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select Nested1 { name } filter .nested2.name = <str>$filter_0'
    assert rendered.context == FrozenDict(filter_0='n2')
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_shape_filter_enumeration(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2(
            Nested1.c.nested2.name,
            Nested1.c.nested2.nested3(
                Nested1.c.nested2.nested3.name,
            ).where(Nested3.c.name == 'n3'),
        ).where(Nested2.c.name == 'n2'),
    ).where(Nested1.c.name == 'n1').build()
    assert rendered.query == (
        'select Nested1 { name, nested2: { name, nested3: { name } '
        'filter .name = <str>$filter_1 } '
        'filter .name = <str>$filter_0 } '
        'filter .name = <str>$filter_2'
    )
    assert rendered.context == FrozenDict(
        filter_0='n2',
        filter_1='n3',
        filter_2='n1',
    )
