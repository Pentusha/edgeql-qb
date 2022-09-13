from types import MappingProxyType

from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.func import Function, math
from edgeql_qb.types import int16, unsafe_text

A = EdgeDBModel('A')
Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')


def test_insert_literals(client: Client) -> None:
    rendered = A.insert.values(p_int16=int16(1), p_str='Hello').all()
    assert rendered.query == (
        'insert A { p_int16 := <int16>$insert_0, p_str := <str>$insert_1 }'
    )
    assert rendered.context == MappingProxyType({'insert_0': 1, 'insert_1': 'Hello'})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


exclamation = Function('default', 'exclamation')


def test_insert_from_function_from_literal(client: Client) -> None:
    rendered = A.insert.values(
        p_int16=math.abs(int16(-1)),
        p_str=exclamation('test'),
    ).all()
    assert rendered.query == (
        'insert A { '
        'p_int16 := math::abs(<int16>$insert_0), '
        'p_str := default::exclamation(<str>$insert_1) '
        '}'
    )
    assert rendered.context == MappingProxyType({
        'insert_0': -1,
        'insert_1': 'test',
    })
    client.query(rendered.query, **rendered.context)
    select = A.select(A.c.p_str, A.c.p_int16).all()
    inserted = client.query(select.query, **select.context)
    assert len(inserted) == 1
    assert inserted[0].p_str == 'test!'
    assert inserted[0].p_int16 == 1


def test_nested_insert(client: Client) -> None:
    rendered = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(
            name='n2',
            nested3=Nested3.insert.values(name='n3'),
        ),
    ).all()
    assert rendered.query == (
        'insert Nested1 { name := <str>$insert_0, nested2 := (insert Nested2 { '
        'name := <str>$insert_1, nested3 := (insert Nested3 { name := '
        '<str>$insert_2 }) }) }'
    )
    assert rendered.context == MappingProxyType({
        'insert_0': 'n1',
        'insert_1': 'n2',
        'insert_2': 'n3',
    })
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_insert_from_select(client: Client) -> None:
    insert = Nested2.insert.values(name='n2').all()
    client.query(insert.query, **insert.context)
    rendered = Nested1.insert.values(
        name='n1',
        nested2=(
            Nested2.select()
            .where(Nested2.c.name == 'n2')
            .limit1
            .offset(unsafe_text('0'))
        ),
    ).all()
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1

    query = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2(Nested1.c.nested2.name),
    ).all()

    result = client.query(query.query, **query.context)
    assert len(result) == 1
    assert result[0].name == 'n1'
    assert result[0].nested2.name == 'n2'
