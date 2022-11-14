from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.types import int16

A = EdgeDBModel('A')


def test_render_delete_all(client: Client) -> None:
    insert = A.insert.values(p_int16=int16(1)).build()
    result = client.query(insert.query, **insert.context)
    assert len(result) == 1
    rendered = A.delete.build()
    assert rendered.query == 'delete A'
    client.query(rendered.query, **rendered.context)
    select = A.select().build()
    result = client.query(select.query, **select.context)
    assert len(result) == 0


def test_delete_filter(client: Client) -> None:
    insert1 = A.insert.values(p_int16=int16(1)).build()
    insert2 = A.insert.values(p_int16=int16(2)).build()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)
    rendered = A.delete.where(A.c.p_int16 == int16(1)).build()
    client.query(rendered.query, **rendered.context)
    select = A.select().build()
    result = client.query(select.query, **select.context)
    assert len(result) == 1


def test_delete_order_offset_limit(client: Client) -> None:
    insert1 = A.insert.values(p_int16=int16(1)).build()
    insert2 = A.insert.values(p_int16=int16(2)).build()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)
    rendered = A.delete.order_by(A.c.p_int16.desc()).offset(1).limit(1).build()
    assert rendered.query == (
        'delete A order by .p_int16 desc offset <int64>$offset_0 limit <int64>$limit_1'
    )
    assert rendered.context == FrozenDict(offset_0=1, limit_1=1)
    client.query(rendered.query, **rendered.context)

    select = A.select().build()
    result = client.query(select.query, **select.context)
    assert len(result) == 1
