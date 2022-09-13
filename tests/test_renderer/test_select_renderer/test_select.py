from types import MappingProxyType

from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel

A = EdgeDBModel('A')
Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')


def test_select_column(client: Client) -> None:
    rendered = A.select(A.c.p_str).all()
    insert = A.insert.values(p_str='Hello').all()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select A { p_str }'
    assert rendered.context == MappingProxyType({})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_nested_select_used(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2(
            Nested1.c.nested2.name,
            Nested1.c.nested3(
                Nested1.c.nested2.nested3.name,
            ).where(Nested3.c.name == 'n3'),
        ),
    ).all()
    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(
            name='n2',
            nested3=Nested3.insert.values(name='n3'),
        ),
    ).all()
    client.query(insert.query, **insert.context)
    assert rendered.query == (
        'select Nested1 {'
        ' name, nested2: {'
        ' name, nested3: { name } filter .name = <str>$filter_0 '
        '} }'
    )
    assert rendered.context == MappingProxyType({'filter_0': 'n3'})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    res = result[0]
    assert res.name == 'n1'
    assert res.nested2.name == 'n2'
    assert res.nested2.nested3.name == 'n3'
