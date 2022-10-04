from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict

Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')


def test_select_exists(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2.exists().label('nested2_exists'),
    ).build()
    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(
            name='n2',
            nested3=Nested3.insert.values(name='n3'),
        ),
    ).build()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select Nested1 { name, nested2_exists := exists .nested2 }'
    assert rendered.context == FrozenDict()
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    res = result[0]
    assert res.name == 'n1'


def test_select_not_exists(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2.not_exists().label('nested2_not_exists'),
    ).build()
    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(name='n2'),
    ).build()
    client.query(insert.query, **insert.context)
    insert = Nested1.insert.values(name='p1').build()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select Nested1 { name, nested2_not_exists := not exists .nested2 }'
    assert rendered.context == FrozenDict()
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 2
