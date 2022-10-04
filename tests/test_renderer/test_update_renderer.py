from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.func import std
from edgeql_qb.types import int16

A = EdgeDBModel('A')
Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')


def test_simple_update(client: Client) -> None:
    insert = A.insert.values(p_int16=int16(1), p_str='Hello').build()
    client.query(insert.query, **insert.context)

    rendered = A.update.values(p_str='New hello').where(A.c.p_int16 == int16(1)).build()
    assert rendered.query == (
        'update A filter .p_int16 = <int16>$filter_0 set { p_str := '
        '<str>$update_1 }'
    )
    assert rendered.context == FrozenDict(update_1='New hello', filter_0=1)

    client.query(rendered.query, **rendered.context)
    select = A.select(A.c.p_str).build()
    result = client.query(select.query, **select.context)
    assert result[0].p_str == 'New hello'


def test_update_with_functions(client: Client) -> None:
    insert = A.insert.values(p_int16=int16(1), p_str='Hello').build()
    client.query(insert.query, **insert.context)
    rendered = (
        A
        .update
        .values(p_int16=std.len(A.c.p_str))
        .where(A.c.p_str == 'Hello')
        .build()
    )
    assert rendered.query == (
        'update A filter .p_str = <str>$filter_0 '
        'set { p_int16 := len(.p_str) }'
    )
    assert rendered.context == FrozenDict(filter_0='Hello')
    result = client.query(insert.query, **insert.context)
    assert len(result) == 1


def test_update_subquery(client: Client) -> None:
    insert = Nested2.insert.values(
        name='new n2',
    ).build()
    client.query(insert.query, **insert.context)

    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(
            name='n2',
        ),
    ).build()
    client.query(insert.query, **insert.context)

    rendered = Nested1.update.values(
        nested2=Nested2.select().where(Nested2.c.name == 'new n2').limit1
    ).build()
    assert rendered.query == (
        'update Nested1 set { nested2 := '
        '(select Nested2 filter .name = <str>$filter_0 limit 1) }'
    )
    assert rendered.context == FrozenDict(filter_0='new n2')

    client.query(rendered.query, **rendered.context)

    select = Nested1.select(Nested1.c.nested2(Nested1.c.nested2.name)).build()
    result = client.query(select.query, **select.context)
    assert len(result) == 1
    assert result[0].nested2.name == 'new n2'


def test_update_existing(client: Client) -> None:
    insert = A.insert.values(p_int16=int16(1), p_str='Hello').build()
    client.query(insert.query, **insert.context)

    rendered = A.update.values(p_int16=A.c.p_int16 + int16(1)).build()
    assert rendered.query == 'update A set { p_int16 := .p_int16 + <int16>$update_0 }'
    assert rendered.context == FrozenDict(update_0=1)
    client.query(rendered.query, **rendered.context)
    select = A.select(A.c.p_int16).build()
    result = client.query(select.query, **select.context)
    assert len(result) == 1
    assert result[0].p_int16 == 2
