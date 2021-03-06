from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.types import int16

A = EdgeDBModel('A')


def test_render_delete_all(client: Client) -> None:
    insert = A.insert.values(p_int16=int16(1)).all()
    result = client.query(insert.query, **insert.context)
    assert len(result) == 1
    rendered = A.delete.all()
    assert rendered.query == 'delete A'
    client.query(rendered.query, **rendered.context)
    select = A.select().all()
    result = client.query(select.query, **select.context)
    assert len(result) == 0


def test_delete_filter(client: Client) -> None:
    insert1 = A.insert.values(p_int16=int16(1)).all()
    insert2 = A.insert.values(p_int16=int16(2)).all()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)
    rendered = A.delete.where(A.c.p_int16 == int16(1)).all()
    client.query(rendered.query, **rendered.context)
    select = A.select().all()
    result = client.query(select.query, **select.context)
    assert len(result) == 1
