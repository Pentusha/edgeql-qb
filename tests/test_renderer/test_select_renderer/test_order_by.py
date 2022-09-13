from types import MappingProxyType

from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.func import math
from edgeql_qb.types import int32, int64

A = EdgeDBModel('A')


def test_simple_select_with_simple_order_by(client: Client) -> None:
    rendered = A.select(A.c.p_str).order_by(A.c.p_str).all()
    insert1 = A.insert.values(p_str='2').all()
    insert2 = A.insert.values(p_str='1').all()
    insert3 = A.insert.values(p_str='3').all()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)
    client.query(insert3.query, **insert3.context)
    assert rendered.query == 'select A { p_str } order by .p_str'
    assert rendered.context == MappingProxyType({})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 3
    assert [row.p_str for row in result] == ['1', '2', '3']


def test_simple_select_with_complex_order_by(client: Client) -> None:
    insert1 = A.insert.values(p_int32=int32(2), p_int64=int64(0), p_bool=False).all()
    insert2 = A.insert.values(p_int32=int32(1), p_int64=int64(0), p_bool=False).all()
    insert3 = A.insert.values(p_int32=int32(3), p_int64=int64(0), p_bool=False).all()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)
    client.query(insert3.query, **insert3.context)
    rendered = (
        A
        .select(A.c.p_str)
        .order_by(
            A.c.p_int64.asc(),
            ((A.c.p_int32 + int32(2)) * A.c.p_int16).desc(),
            ~A.c.p_bool,
            math.floor(A.c.p_int32),
        )
        .all()
    )
    assert rendered.query == (
        'select A { p_str } '
        'order by '
        '.p_int64 asc '
        'then (.p_int32 + <int32>$order_by_0) * .p_int16 desc '
        'then not .p_bool '
        'then math::floor(.p_int32)'
    )
    assert rendered.context == MappingProxyType({'order_by_0': 2})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 3
