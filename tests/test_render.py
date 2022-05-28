from contextlib import suppress
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Iterator, Any

import pytest
from edgedb.blocking_client import Client, create_client

from edgeql_qb import EdgeDBModel
from edgeql_qb.operators import Column
from edgeql_qb.types import bigint, float32, float64, int16, int32, int64, GenericHolder

A = EdgeDBModel('A')
Nested1 = EdgeDBModel('Nested1')
Nested2 = EdgeDBModel('Nested2')
Nested3 = EdgeDBModel('Nested3')


class Rollback(Exception):
    pass


@pytest.fixture
def client() -> Iterator[Client]:
    client = create_client()
    with suppress(Rollback):
        for tx in client.transaction():
            with tx:
                yield tx
                raise Rollback


def test_select_column(client: Client) -> None:
    rendered = A.select(A.c.p_str).all()
    client.query('insert A { p_str := "Hello" }')
    assert rendered.query == 'select A { p_str }'
    assert rendered.context == {}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


_dt = datetime(2022, 5, 26, 0, 0, 0)


@pytest.mark.parametrize(
    'label, column, value, expected_type',
    (
        ('bool_exp', A.c.p.p_bool, False, 'bool'),
        ('str_exp', A.c.p.p_str, 'Hello', 'str'),
        ('datetime_exp', A.c.p.p_local_datetime, _dt.replace(tzinfo=timezone.utc), 'datetime'),
        ('local_datetime_exp', A.c.p.p_local_dattime, _dt, 'cal::local_datetime'),
        ('local_date_exp', A.c.p.p_local_date, _dt.date(), 'cal::local_date'),
        ('local_time_exp', A.c.p.p_local_time, _dt.time(), 'cal::local_time'),
        ('duration_exp', A.c.p.p_duration, timedelta(), 'duration'),
        ('int32_exp', A.c.p.p_int32, int32(1), 'int32'),
        ('int64_exp', A.c.p.p_int64, int64(1), 'int64'),
        ('bigint_exp', A.c.p.p_bigint, bigint(1), 'bigint'),
        ('float32_exp', A.c.p.p_float32, float32(1), 'float32'),
        ('float64_exp', A.c.p.p_float64, float64(1), 'float64'),
        ('decimal_exp', A.c.p.p_decimal, Decimal(1), 'decimal'),
        ('bytes_exp', A.c.p.p_bytes, b'Hello', 'bytes'),
    ),
)
def test_select_datatypes(
    client: Client,
    label: str,
    column: Column,
    value: Any,
    expected_type: str,
) -> None:
    rendered = A.select((column != value).label(label)).all()
    assert rendered.query == (
        f'select A {{ {label} := .{column.column_name} != <{expected_type}>$select_0_0 }}'
    )
    assert rendered.context == {
        'select_0_0': isinstance(value, GenericHolder) and value.value or value,
    }


def test_select_operators(client: Client) -> None:
    rendered = A.select(
        (A.c.p_bool & True).label('and_result'),
        (A.c.p_bool | True).label('or_result'),
        (A.c.p_int32 / A.c.p_int32).label('true_div_result'),
        (A.c.p_int32 // A.c.p_int32).label('floor_div_result'),
        (A.c.p_int32 % A.c.p_int32).label('mod_result'),
    ).all()
    client.query('insert A { p_bool := True, p_int32 := 10 }')
    assert rendered.query == (
        'select A { '
        'and_result := .p_bool and <bool>$select_0_0, '
        'or_result := .p_bool or <bool>$select_1_0, '
        'true_div_result := .p_int32 / .p_int32, '
        'floor_div_result := .p_int32 // .p_int32, '
        'mod_result := .p_int32 % .p_int32 '
        '}'
    )
    assert rendered.context == {'select_0_0': True, 'select_1_0': True}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_select_concatenation(client: Client) -> None:
    rendered = A.select(A.c.p_str.op('++')(A.c.p_str).label('result')).all()
    client.query('insert A { p_str := "Hello" }')
    assert rendered.query == 'select A { result := .p_str ++ .p_str }'
    assert rendered.context == {}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].result == 'HelloHello'


def test_select_unary_expression(client: Client) -> None:
    rendered = A.select((-A.c.p_int32).label('minus_p_int32')).all()
    client.query('insert A { p_int32 := 5 }')
    assert rendered.query == 'select A { minus_p_int32 := -.p_int32 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].minus_p_int32 == -5


def test_select_left_parentheses(client: Client) -> None:
    client.query('insert A { p_int32 := 5 }')
    rendered = A.select(((A.c.p_int32 + A.c.p_int32) * A.c.p_int32).label('result')).all()
    assert rendered.query == 'select A { result := (.p_int32 + .p_int32) * .p_int32 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].result == 50


def test_simple_select_with_simple_order_by(client: Client) -> None:
    rendered = A.select(A.c.p_str).order_by(A.c.p_str).all()
    client.query('insert A { p_str := "2" }')
    client.query('insert A { p_str := "1" }')
    client.query('insert A { p_str := "3" }')
    assert rendered.query == 'select A { p_str } order by .p_str'
    assert rendered.context == {}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 3
    assert [row.p_str for row in result] == ['1', '2', '3']


def test_simple_select_with_complex_order_by(client: Client) -> None:
    client.query('insert A { p_int32 := 2, p_int64 := 0, p_bool := False }')
    client.query('insert A { p_int32 := 1, p_int64 := 0, p_bool := False }')
    client.query('insert A { p_int32 := 3, p_int64 := 0, p_bool := False }')
    rendered = (
        A
        .select(A.c.p_str)
        .order_by(
            A.c.p_int64.asc(),
            ((A.c.p_int32 + int32(2)) * A.c.p_int16).desc(),
            ~A.c.p_bool,
        )
        .all()
    )
    assert rendered.query == (
        'select A { p_str } '
        'order by '
        '.p_int64 asc '
        'then (.p_int32 + <int32>$order_by_1_1) * .p_int16 desc '
        'then not .p_bool'
    )
    assert rendered.context == {'order_by_1_1': 2}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 3


def test_nested_select_used(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2.select(
            Nested1.c.nested2.name,
            Nested1.c.nested3.select(
                Nested1.c.nested2.nested3.name,
            ),
        ),
    ).all()
    client.query(
        'insert Nested1 { name := "n1", nested2 := ('
        'insert Nested2 { name := "n2", nested3 := ('
        'insert Nested3 { name := "n3" }) }) }'
    )
    assert rendered.query == 'select Nested1 { name, nested2: { name, nested3: { name } } }'
    assert rendered.context == {}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    res = result[0]
    assert res.name == 'n1'
    assert res.nested2.name == 'n2'
    assert res.nested2.nested3.name == 'n3'


def test_select_exists(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2.exists().label('nested2_exists'),
    ).all()
    client.query(
        'insert Nested1 { name := "n1", nested2 := ('
        'insert Nested2 { name := "n2", nested3 := ('
        'insert Nested3 { name := "n3" }) }) }'
    )
    assert rendered.query == 'select Nested1 { name, nested2_exists := exists .nested2 }'
    assert rendered.context == {}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    res = result[0]
    assert res.name == 'n1'


def test_select_not_exists(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2.not_exists().label('nested2_not_exists'),
    ).all()
    client.query(
        'insert Nested1 { name := "n1", nested2 := ('
        'insert Nested2 { name := "n2" }) }'
    )
    client.query(
        'insert Nested1 { name := "p1" }'
    )
    assert rendered.query == 'select Nested1 { name, nested2_not_exists := not exists .nested2 }'
    assert rendered.context == {}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 2


def test_complex_filter_with_literal(client: Client) -> None:
    client.query('insert A { p_int64 := 11, p_int32:= 1 }')
    rendered = (
        A.select(A.c.p_int64)
        .where(A.c.p_int64 >= int64(10))
        .where(A.c.p_int32 <= int32(15))
        .where((A.c.p_int32 + A.c.p_int64) * int64(5) > int64(50))
        .where(A.c.p_int32 * (A.c.p_int64 + int64(10)) > int64(20))
        .where(A.c.p_int32 < A.c.p_int64)
        .where(A.c.p_int64 != int64(1))
        .all()
    )
    assert rendered.query == (
        'select A { p_int64 } '
        'filter '
        '.p_int64 >= <int64>$filter_0_0 '
        'and .p_int32 <= <int32>$filter_1_0 '
        'and (.p_int32 + .p_int64) * <int64>$filter_2_1 > <int64>$filter_2_0 '
        'and .p_int32 * (.p_int64 + <int64>$filter_3_1) > <int64>$filter_3_0 '
        'and .p_int32 < .p_int64 '
        'and .p_int64 != <int64>$filter_5_0'
    )
    assert rendered.context == {
        'filter_0_0': 10,
        'filter_1_0': 15,
        'filter_2_0': 50,
        'filter_2_1': 5,
        'filter_3_0': 20,
        'filter_3_1': 10,
        'filter_5_0': 1,
    }
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_filter_with_unary_op(client: Client) -> None:
    client.query('insert A { p_int64 := 11, p_bool := False }')
    rendered = (
        A.select(A.c.p_int64)
        .where(~A.c.p_bool)
        .all()
    )
    assert rendered.query == 'select A { p_int64 } filter not .p_bool'
    assert rendered.context == {}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].p_int64 == 11


def test_nested_filter(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
    ).where(Nested1.c.nested2.name == 'n2').all()
    client.query(
        'insert Nested1 { name := "n1", nested2 := (insert Nested2 { name := "n2" }) }'
    )
    assert rendered.query == 'select Nested1 { name } filter .nested2.name = <str>$filter_0_0'
    assert rendered.context == {'filter_0_0': 'n2'}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_select_expression(client: Client) -> None:
    client.query('insert A { p_int16 := 16, p_int32 := 4, p_int64 := 11 }')
    rendered = A.select((A.c.p_int64 + A.c.p_int64).label('my_sum')).all()
    assert rendered.query == 'select A { my_sum := .p_int64 + .p_int64 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].my_sum == 22

    rendered = A.select((A.c.p_int16 + A.c.p_int32 == A.c.p_int64).label('is_equal')).all()
    assert rendered.query == 'select A { is_equal := .p_int16 + .p_int32 = .p_int64 }'


def test_select_expression_with_literal(client: Client) -> None:
    client.query('insert A { p_int64 := 11 }')
    rendered = A.select((A.c.p_int64 + int64(1)).label('my_sum')).all()
    assert rendered.query == 'select A { my_sum := .p_int64 + <int64>$select_0_0 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].my_sum == 12


def test_select_binary_optimisations() -> None:
    rendered = A.select((A.c.p_int64 - -A.c.p_int64).label('my_sum')).all()
    assert rendered.query == 'select A { my_sum := .p_int64 + .p_int64 }'

    rendered = A.select((A.c.p_int64 + -A.c.p_int64).label('my_sum')).all()
    assert rendered.query == 'select A { my_sum := .p_int64 - .p_int64 }'

    rendered = A.select((A.c.p_int64 - +A.c.p_int64).label('my_sum')).all()
    assert rendered.query == 'select A { my_sum := .p_int64 - .p_int64 }'


def test_limit_offset(client: Client) -> None:
    client.query('insert A { p_int16 := 1 }')
    client.query('insert A { p_int16 := 2 }')
    client.query('insert A { p_int16 := 3 }')
    client.query('insert A { p_int16 := 4 }')
    client.query('insert A { p_int16 := 5 }')
    rendered = (
        A.select(A.c.p_int16)
        .order_by(A.c.p_int16.asc())
        .limit(2)
        .offset(4)
        .all()
    )
    assert rendered.query == (
        'select A { p_int16 } '
        'order by .p_int16 asc '
        'offset <int64>$offset '
        'limit <int64>$limit'
    )
    assert rendered.context == {'limit': 2, 'offset': 4}
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].p_int16 == 5


def test_render_delete_all(client: Client) -> None:
    client.query('insert A { p_int16 := 1 }')
    rendered = A.delete.all()
    assert rendered.query == 'delete A'
    client.query(rendered.query, **rendered.context)
    select = A.select().all()
    result = client.query(select.query, **select.context)
    assert len(result) == 0


def test_delete_filter(client: Client) -> None:
    client.query('insert A { p_int16 := 1 }')
    client.query('insert A { p_int16 := 2 }')
    rendered = A.delete.where(A.c.p_int16 == int16(1)).all()
    client.query(rendered.query, **rendered.context)
    select = A.select().all()
    result = client.query(select.query, **select.context)
    assert len(result) == 1
