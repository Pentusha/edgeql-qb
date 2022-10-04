from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.operators import Alias
from edgeql_qb.types import int64

A = EdgeDBModel('A')
TestA = EdgeDBModel('A', module='test_module')


def test_select_with_literal_with(client: Client) -> None:
    insert = A.insert.values(p_int64=int64(1)).build()
    client.query(insert.query, **insert.context)

    x = Alias('x').assign(int64(1))
    rendered = A.select(
        A.c.p_int64,
        (A.c.p_int64 + x).label('y'),
    ).with_(x).build()
    assert rendered.query == (
        'with x := <int64>$with_0 select A { p_int64, y := .p_int64 + x }'
    )
    assert rendered.context == FrozenDict(with_0=1)

    result = client.query(rendered.query, **rendered.context)
    assert result[0].p_int64 == 1
    assert result[0].y == 2


def test_select_with_literal_and_module() -> None:
    x = Alias('x').assign(int64(1))
    rendered = TestA.select(
        TestA.c.p_int64,
        (TestA.c.p_int64 + x).label('y'),
    ).with_(x).build()
    assert rendered.query == (
        'with test_module, x := <int64>$with_0 select A { p_int64, y := .p_int64 + x }'
    )
    assert rendered.context == FrozenDict(with_0=1)


def test_select_with_module_and_without_expressions() -> None:
    rendered = TestA.select(TestA.c.p_int64).build()
    assert rendered.query == (
        'with test_module select A { p_int64 }'
    )
    assert rendered.context == FrozenDict()
