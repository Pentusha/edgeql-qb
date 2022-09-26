from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict

A = EdgeDBModel('A')
Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')
WithConstraints = EdgeDBModel('WithConstraints')


def test_select_column(client: Client) -> None:
    rendered = A.select(A.c.p_str).all()
    insert = A.insert.values(p_str='Hello').all()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select A { p_str }'
    assert rendered.context == FrozenDict()
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_nested_shape_used(client: Client) -> None:
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
    assert rendered.context == FrozenDict(filter_0='n3')
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    res = result[0]
    assert res.name == 'n1'
    assert res.nested2.name == 'n2'
    assert res.nested2.nested3.name == 'n3'


def test_nested_query(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        nested_a=A.select().where(A.c.p_str == 'test').limit1,
    ).all()
    assert rendered.query == (
        'select Nested1 { name, nested_a := (select A filter .p_str = <str>$filter_0 limit 1) }'
    )
    assert rendered.context == FrozenDict(filter_0='test')
    result = client.query(rendered.query, **rendered.context)
    assert not result


def test_select_from_insert(client: Client) -> None:
    rendered = (
        WithConstraints
        .select(WithConstraints.c.name)
        .select_from(
            WithConstraints
            .insert
            .values(name='test')
            .unless_conflict(WithConstraints.c.name, WithConstraints)
        )
        .all()
    )
    assert rendered.query == (
        'select ('
        'insert WithConstraints { name := <str>$insert_0 } '
        'unless conflict on .name else WithConstraints'
        ') { name }'
    )
    assert rendered.context == FrozenDict(insert_0='test')
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].name == 'test'
