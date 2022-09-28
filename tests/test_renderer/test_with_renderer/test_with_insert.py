from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.operators import Alias
from edgeql_qb.types import int64

A = EdgeDBModel('A')
Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')


def test_insert_with_literal_with(client: Client) -> None:
    x = Alias('x').assign(int64(1))
    rendered = A.insert.values(p_int64=x).with_(x).all()
    assert rendered.query == 'with x := <int64>$with_0 insert A { p_int64 := x }'
    assert rendered.context == FrozenDict(with_0=1)
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_nested_insert_from_with_expressions(client: Client) -> None:
    nested3_insert = Nested3.insert.values(name='n3').label('n3_insert')
    nested2_insert = Nested2.insert.values(name='n2', nested3=nested3_insert).label('n2_insert')
    rendered = (
        Nested1
        .insert
        .with_(nested3_insert, nested2_insert)
        .values(name='n1', nested2=nested2_insert)
        .all()
    )
    assert rendered.query == (
        'with n3_insert := (insert Nested3 { name := <str>$insert_0 }), n2_insert := '
        '(insert Nested2 { name := <str>$insert_1, nested3 := n3_insert }) insert '
        'Nested1 { name := <str>$insert_2, nested2 := n2_insert }'
    )
    assert rendered.context == FrozenDict(
        insert_0='n3',
        insert_1='n2',
        insert_2='n1',
    )
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
