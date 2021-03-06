from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.types import int16, text

A = EdgeDBModel('A')
Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')


def test_simple_update(client: Client) -> None:
    insert = A.insert.values(p_int16=int16(1), p_str='Hello').all()
    client.query(insert.query, **insert.context)

    rendered = A.update.values(p_str='New hello').where(A.c.p_int16 == int16(1)).all()
    assert rendered.query == (
        'update A filter .p_int16 = <int16>$filter_1_0_0 set { p_str := '
        '<str>$update_1_0_0 }'
    )
    assert rendered.context == {'update_1_0_0': 'New hello', 'filter_1_0_0': 1}

    client.query(rendered.query, **rendered.context)
    select = A.select(A.c.p_str).all()
    result = client.query(select.query, **select.context)
    assert result[0].p_str == 'New hello'


def test_update_subquery(client: Client) -> None:
    insert = Nested2.insert.values(
        name='new n2',
    ).all()
    client.query(insert.query, **insert.context)

    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(
            name='n2',
        ),
    ).all()
    client.query(insert.query, **insert.context)

    rendered = Nested1.update.values(
        nested2=(
            Nested2.select()
            .where(Nested2.c.name == 'new n2')
            .limit(text('1'))
        )
    ).all()
    assert rendered.query == (
        'update Nested1 set { nested2 := '
        '(select Nested2 filter .name = <str>$filter_2_0_0 limit 1) }'
    )
    assert rendered.context == {'filter_2_0_0': 'new n2'}

    client.query(rendered.query, **rendered.context)

    select = Nested1.select(Nested1.c.nested2.select(Nested1.c.nested2.name)).all()
    result = client.query(select.query, **select.context)
    assert len(result) == 1
    assert result[0].nested2.name == 'new n2'
