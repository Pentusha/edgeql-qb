from types import MappingProxyType

from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.func import math
from edgeql_qb.types import int16

A = EdgeDBModel('A')


def test_limit_offset(client: Client) -> None:
    for num in range(1, 6):
        insert = A.insert.values(p_int16=int16(num)).all()
        client.query(insert.query, **insert.context)
    rendered = (
        A.select(A.c.p_int16)
        .order_by(A.c.p_int16.asc())
        .limit(2)
        .offset(4)
        .all()
    )
    assert rendered.query == (
        'select A { p_int16 } '
        'order by .p_int16 asc '
        'offset <int64>$offset_0 '
        'limit <int64>$limit_1'
    )
    assert rendered.context == MappingProxyType({'limit_1': 2, 'offset_0': 4})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].p_int16 == 5


def test_limit_offset_function(client: Client) -> None:
    for num in range(1, 6):
        insert = A.insert.values(p_int16=int16(num)).all()
        client.query(insert.query, **insert.context)
    rendered = (
        A
        .select(A.c.p_int16)
        .order_by(A.c.p_int16.asc())
        .limit(math.abs(int16(-2)))
        .offset(math.abs(int16(-4)))
        .all()
    )
    assert rendered.query == (
        'select A { p_int16 } order by .p_int16 asc '
        'offset math::abs(<int16>$offset_0) '
        'limit math::abs(<int16>$limit_0)'
    )
    assert rendered.context == MappingProxyType({'limit_0': -2, 'offset_0': -4})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
