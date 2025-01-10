from multiprocessing.connection import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.types import int16

A = EdgeDBModel('A')


def test_count_wo_filters() -> None:
    rendered = A.count.build()
    assert rendered.query == 'select count(A)'


def test_count_filter(client: Client) -> None:
    insert1 = A.insert.values(p_int16=int16(1)).build()
    insert2 = A.insert.values(p_int16=int16(2)).build()
    insert3 = A.insert.values(p_int16=int16(3)).build()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)
    client.query(insert3.query, **insert3.context)
    rendered = A.count.where(A.c.p_int16 <= int16(2)).build()
    assert rendered.query == 'select count((select A filter .p_int16 <= <int16>$filter_0))'
    assert rendered.context == FrozenDict(filter_0=2)
    result = client.query(rendered.query, **rendered.context)
    assert result == [2]
