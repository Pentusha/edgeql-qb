import pytest
from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.func import std
from edgeql_qb.types import int16, int32

A = EdgeDBModel('A')


@pytest.fixture
def bootstrap(client: Client) -> None:
    insert1 = A.insert.values(p_int16=int16(1), p_int32=int32(100), p_str='1').build()
    insert2 = A.insert.values(p_int16=int16(1), p_int32=int32(200), p_str='2').build()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)


def test_group_statement_wo_columns(bootstrap: None) -> None:
    rendered = A.group().by(A.c.p_int16).build()
    assert rendered.query == 'group A by .p_int16'
    assert rendered.context == FrozenDict()


def test_simple_group_statement(client: Client, bootstrap: None) -> None:
    rendered = A.group(A.c.p_str).by(A.c.p_int16).build()
    assert rendered.query == 'group A { p_str } by .p_int16'
    assert rendered.context == FrozenDict()

    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_group_with_using(client: Client, bootstrap: None) -> None:
    a = (A.c.p_int16 + int16(1)).label('a')
    b = (a + int16(2)).label('b')
    c = (a + b).label('c')

    rendered = A.group(A.c.p_str).using(a, b, c).by(c).build()
    assert rendered.query == (
        'group A { p_str } '
        'using a := .p_int16 + <int16>$using_0, b := a + <int16>$using_1, c := a + b '
        'by c'
    )
    assert rendered.context == FrozenDict(using_0=1, using_1=2)
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_group_with_unary_using(bootstrap: None) -> None:
    condition = (~A.c.p_bool).label('condition')
    rendered = A.group().using(condition).by(condition).build()
    assert rendered.query == 'group A using condition := not .p_bool by condition'
    assert rendered.context == FrozenDict()


def test_group_with_function_in_select(client: Client, bootstrap: None) -> None:
    rendered = (
        A
        .group(A.c.p_str, std.to_str(A.c.p_int32).label('str_int32'))
        .by(A.c.p_int16)
        .build()
    )
    assert rendered.query == (
        'group A { p_str, str_int32 := to_str(.p_int32) } '
        'by .p_int16'
    )
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert len(result[0].elements) == 2
    assert result[0].elements[0].str_int32 == '100'
    assert result[0].elements[1].str_int32 == '200'


def test_group_with_function_in_using(client: Client, bootstrap: None) -> None:
    str_int16 = std.to_str(A.c.p_int16).label('str_int16')
    str_len = std.len(str_int16).label('str_len')
    rendered = (
        A
        .group(A.c.p_str)
        .using(str_int16, str_len)
        .by(str_len)
        .build()
    )
    assert rendered.query == (
        'group A { p_str } '
        'using str_int16 := to_str(.p_int16), str_len := len(str_int16) '
        'by str_len'
    )
    assert rendered.context == FrozenDict()
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
