from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.operators import Alias
from edgeql_qb.types import int64

A = EdgeDBModel('A')


def test_insert_with_literal_with(client: Client) -> None:
    x = Alias('x').assign(int64(1))
    rendered = A.insert.values(p_int64=x).with_(x).all()
    assert rendered.query == 'with x := <int64>$with_0 insert A { p_int64 := x }'
    assert rendered.context == FrozenDict(with_0=1)
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
