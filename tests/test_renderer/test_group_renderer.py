from types import MappingProxyType, NoneType

import pytest
from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.types import int16

A = EdgeDBModel('A')


@pytest.fixture
def bootstrap(client: Client) -> None:
    insert1 = A.insert.values(p_int16=int16(1), p_str='1').all()
    insert2 = A.insert.values(p_int16=int16(1), p_str='2').all()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)


def test_group_statement_wo_columns(bootstrap: NoneType) -> None:
    rendered = A.group().by(A.c.p_int16).all()
    assert rendered.query == 'group A by .p_int16'
    assert rendered.context == MappingProxyType({})


def test_simple_group_statement(client: Client, bootstrap: NoneType) -> None:
    rendered = A.group(A.c.p_str).by(A.c.p_int16).all()
    assert rendered.query == 'group A { p_str } by .p_int16'
    assert rendered.context == MappingProxyType({})

    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_group_with_using(client: Client, bootstrap: NoneType) -> None:
    a = (A.c.p_int16 + int16(1)).label('a')
    b = (a + int16(2)).label('b')
    c = (a + b).label('c')

    rendered = A.group(A.c.p_str).using(a, b, c).by(c).all()
    assert rendered.query == (
        'group A { p_str } '
        'using a := .p_int16 + <int16>$using_0_0_0, b := a + <int16>$using_0_1_0, c := a + b '
        'by c'
    )
    assert rendered.context == MappingProxyType({'using_0_0_0': 1, 'using_0_1_0': 2})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_group_with_unary_using(bootstrap: NoneType) -> None:
    condition = (~A.c.p_bool).label('condition')
    rendered = A.group().using(condition).by(condition).all()
    assert rendered.query == 'group A using condition := not .p_bool by condition'
    assert rendered.context == MappingProxyType({})
