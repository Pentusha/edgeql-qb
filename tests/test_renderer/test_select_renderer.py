from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import MappingProxyType
from typing import Any

import pytest
from edgedb.blocking_client import Client

from edgeql_qb import EdgeDBModel
from edgeql_qb.func import FuncInvocation, math, std
from edgeql_qb.operators import BinaryOp, Column
from edgeql_qb.types import (
    GenericHolder,
    bigint,
    float32,
    float64,
    int16,
    int32,
    int64,
    unsafe_text,
)

_dt = datetime(2022, 5, 26, 0, 0, 0)
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


@pytest.mark.parametrize(
    ['label', 'column', 'value', 'expected_type'],
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
    label: str,
    column: Column,
    value: Any,
    expected_type: str,
) -> None:
    rendered = A.select((column != value).label(label)).all()
    assert rendered.query == (
        f'select A {{ {label} := .{column.column_name} != <{expected_type}>$select_0_0_0 }}'
    )
    assert rendered.context == MappingProxyType({
        'select_0_0_0': isinstance(value, GenericHolder) and value.value or value,
    })


def test_select_operators(client: Client) -> None:
    rendered = A.select(
        (A.c.p_bool & True).label('and_result'),
        (A.c.p_bool | True).label('or_result'),
        (A.c.p_int32 / A.c.p_int64).label('true_div_result'),
        (A.c.p_int32 // A.c.p_int64).label('floor_div_result'),
        (A.c.p_int32 % A.c.p_int64).label('mod_result'),
        (math.abs(A.c.p_int32 - int32(20)) + int32(1)).label('abs')
    ).all()
    insert = A.insert.values(p_bool=True, p_int32=int32(10)).all()
    client.query(insert.query, **insert.context)
    assert rendered.query == (
        'select A { '
        'and_result := .p_bool and <bool>$select_0_0_0, '
        'or_result := .p_bool or <bool>$select_0_1_0, '
        'true_div_result := .p_int32 / .p_int64, '
        'floor_div_result := .p_int32 // .p_int64, '
        'mod_result := .p_int32 % .p_int64, '
        'abs := math::abs(.p_int32 - <int32>$select_0_5_1) + <int32>$select_0_5_0 '
        '}'
    )
    assert rendered.context == MappingProxyType({
        'select_0_0_0': True,
        'select_0_1_0': True,
        'select_0_5_0': 1,
        'select_0_5_1': 20,
    })
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_select_concatenation(client: Client) -> None:
    rendered = A.select(A.c.p_str.op('++')(A.c.p_str).label('result')).all()
    insert = A.insert.values(p_str='Hello').all()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select A { result := .p_str ++ .p_str }'
    assert rendered.context == MappingProxyType({})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].result == 'HelloHello'


def test_select_like_ilike(client: Client) -> None:
    insert_lower = A.insert.values(p_str='hello').all()
    insert_upper = A.insert.values(p_str='HELLO').all()
    client.query(insert_lower.query, **insert_lower.context)
    client.query(insert_upper.query, **insert_upper.context)
    select = A.select(A.c.p_str).where(A.c.p_str.like('%ell%')).all()
    result = client.query(select.query, **select.context)
    assert len(result) == 1
    assert result[0].p_str == 'hello'

    select = A.select(A.c.p_str).where(A.c.p_str.ilike('%ell%')).all()
    result = client.query(select.query, **select.context)
    assert len(result) == 2


def test_select_unary_expression(client: Client) -> None:
    rendered = A.select((-A.c.p_int32).label('minus_p_int32')).all()
    insert = A.insert.values(p_int32=int32(5)).all()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select A { minus_p_int32 := -.p_int32 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].minus_p_int32 == -5


def test_select_left_parentheses(client: Client) -> None:
    insert = A.insert.values(p_int32=int32(5)).all()
    client.query(insert.query, **insert.context)
    rendered = A.select(((A.c.p_int32 + A.c.p_int32) * A.c.p_int32).label('result')).all()
    assert rendered.query == 'select A { result := (.p_int32 + .p_int32) * .p_int32 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].result == 50


def test_simple_select_with_simple_order_by(client: Client) -> None:
    rendered = A.select(A.c.p_str).order_by(A.c.p_str).all()
    insert1 = A.insert.values(p_str='2').all()
    insert2 = A.insert.values(p_str='1').all()
    insert3 = A.insert.values(p_str='3').all()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)
    client.query(insert3.query, **insert3.context)
    assert rendered.query == 'select A { p_str } order by .p_str'
    assert rendered.context == MappingProxyType({})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 3
    assert [row.p_str for row in result] == ['1', '2', '3']


def test_simple_select_with_complex_order_by(client: Client) -> None:
    insert1 = A.insert.values(p_int32=int32(2), p_int64=int64(0), p_bool=False).all()
    insert2 = A.insert.values(p_int32=int32(1), p_int64=int64(0), p_bool=False).all()
    insert3 = A.insert.values(p_int32=int32(3), p_int64=int64(0), p_bool=False).all()
    client.query(insert1.query, **insert1.context)
    client.query(insert2.query, **insert2.context)
    client.query(insert3.query, **insert3.context)
    rendered = (
        A
        .select(A.c.p_str)
        .order_by(
            A.c.p_int64.asc(),
            ((A.c.p_int32 + int32(2)) * A.c.p_int16).desc(),
            ~A.c.p_bool,
            math.floor(A.c.p_int32),
        )
        .all()
    )
    assert rendered.query == (
        'select A { p_str } '
        'order by '
        '.p_int64 asc '
        'then (.p_int32 + <int32>$order_by_1_1) * .p_int16 desc '
        'then not .p_bool '
        'then math::floor(.p_int32)'
    )
    assert rendered.context == MappingProxyType({'order_by_1_1': 2})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 3


def test_nested_select_used(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2(
            Nested1.c.nested2.name,
            Nested1.c.nested3(
                Nested1.c.nested2.nested3.name,
            ),
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
    assert rendered.query == 'select Nested1 { name, nested2: { name, nested3: { name } } }'
    assert rendered.context == MappingProxyType({})
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
    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(
            name='n2',
            nested3=Nested3.insert.values(name='n3'),
        ),
    ).all()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select Nested1 { name, nested2_exists := exists .nested2 }'
    assert rendered.context == MappingProxyType({})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    res = result[0]
    assert res.name == 'n1'


def test_select_not_exists(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
        Nested1.c.nested2.not_exists().label('nested2_not_exists'),
    ).all()
    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(name='n2'),
    ).all()
    client.query(insert.query, **insert.context)
    insert = Nested1.insert.values(name='p1').all()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select Nested1 { name, nested2_not_exists := not exists .nested2 }'
    assert rendered.context == MappingProxyType({})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 2


@pytest.mark.parametrize(
    ['condition', 'expected_condition', 'expected_context'],
    (
        (A.c.p_int32 < A.c.p_int64, '.p_int32 < .p_int64', {}),
        (A.c.p_int64 != int64(1), '.p_int64 != <int64>$filter_1_0_0', {'filter_1_0_0': 1}),
        (A.c.p_int64 >= int64(1), '.p_int64 >= <int64>$filter_1_0_0', {'filter_1_0_0': 1}),
        (A.c.p_int32 <= int32(1), '.p_int32 <= <int32>$filter_1_0_0', {'filter_1_0_0': 1}),
        (
            (A.c.p_int32 + A.c.p_int64) * int64(1) > int64(2),
            '(.p_int32 + .p_int64) * <int64>$filter_1_0_1 > <int64>$filter_1_0_0',
            {'filter_1_0_1': 1, 'filter_1_0_0': 2},
        ),
        (
            A.c.p_int32 * (A.c.p_int64 + int64(1)) > int64(2),
            '.p_int32 * (.p_int64 + <int64>$filter_1_0_1) > <int64>$filter_1_0_0',
            {'filter_1_0_1': 1, 'filter_1_0_0': 2},
        ),
        (A.c.p_str == unsafe_text("'Hello'"), ".p_str = 'Hello'", {}),
        (
            std.contains(A.c.p_str, 'He'),
            'contains(.p_str, <str>$filter_1_0_0)',
            {'filter_1_0_0': 'He'},
        ),
    ),
)
def test_complex_filter_with_literal(
    client: Client,
    condition: BinaryOp | FuncInvocation,
    expected_condition: str,
    expected_context: dict[str, Any],
) -> None:
    insert = A.insert.values(p_int64=int64(11), p_int32=int32(1), p_str='Hello').all()
    client.query(insert.query, **insert.context)
    rendered = A.select(A.c.p_int64).where(condition).all()
    assert rendered.query == f'select A {{ p_int64 }} filter {expected_condition}'
    assert rendered.context == MappingProxyType(expected_context)
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_filter_with_unary_op(client: Client) -> None:
    insert = A.insert.values(p_int64=int64(11), p_bool=False).all()
    client.query(insert.query, **insert.context)
    rendered = (
        A.select(A.c.p_int64)
        .where(~A.c.p_bool)
        .all()
    )
    assert rendered.query == 'select A { p_int64 } filter not .p_bool'
    assert rendered.context == MappingProxyType({})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].p_int64 == 11


def test_nested_filter(client: Client) -> None:
    rendered = Nested1.select(
        Nested1.c.name,
    ).where(Nested1.c.nested2.name == 'n2').all()
    insert = Nested1.insert.values(
        name='n1',
        nested2=Nested2.insert.values(name='n2'),
    ).all()
    client.query(insert.query, **insert.context)
    assert rendered.query == 'select Nested1 { name } filter .nested2.name = <str>$filter_1_0_0'
    assert rendered.context == MappingProxyType({'filter_1_0_0': 'n2'})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1


def test_select_expression(client: Client) -> None:
    insert = A.insert.values(p_int16=int16(16), p_int32=int32(4), p_int64=int64(11)).all()
    client.query(insert.query, **insert.context)
    rendered = A.select((A.c.p_int64 + A.c.p_int64).label('my_sum')).all()
    assert rendered.query == 'select A { my_sum := .p_int64 + .p_int64 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].my_sum == 22

    rendered = A.select((A.c.p_int16 + A.c.p_int32 == A.c.p_int64).label('is_equal')).all()
    assert rendered.query == 'select A { is_equal := .p_int16 + .p_int32 = .p_int64 }'


def test_select_expression_with_literal(client: Client) -> None:
    insert = A.insert.values(p_int64=int64(11)).all()
    client.query(insert.query, **insert.context)
    rendered = A.select((A.c.p_int64 + int64(1)).label('my_sum')).all()
    assert rendered.query == 'select A { my_sum := .p_int64 + <int64>$select_0_0_0 }'
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].my_sum == 12


@pytest.mark.parametrize(
    ['expression', 'expected_query'],
    (
        (A.c.p_int64 - -A.c.p_int64, 'select A { my_sum := .p_int64 + .p_int64 }'),
        (A.c.p_int64 + -A.c.p_int64, 'select A { my_sum := .p_int64 - .p_int64 }'),
        (A.c.p_int64 - +A.c.p_int64, 'select A { my_sum := .p_int64 - .p_int64 }'),
    ),
)
def test_select_binary_optimisations(expression: BinaryOp, expected_query: str) -> None:
    rendered = A.select(expression.label('my_sum')).all()
    assert rendered.query == expected_query


def test_limit_offset(client: Client) -> None:
    for num in range(1, 6):
        insert = A.insert.values(p_int16=int16(num)).all()
        client.query(insert.query, **insert.context)
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
        'offset <int64>$offset_0 '
        'limit <int64>$limit_0'
    )
    assert rendered.context == MappingProxyType({'limit_0': 2, 'offset_0': 4})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
    assert result[0].p_int16 == 5


def test_limit_offset_function(client: Client) -> None:
    for num in range(1, 6):
        insert = A.insert.values(p_int16=int16(num)).all()
        client.query(insert.query, **insert.context)
    rendered = (
        A
        .select(A.c.p_int16)
        .order_by(A.c.p_int16.asc())
        .limit(math.abs(int16(-2)))
        .offset(math.abs(int16(-4)))
        .all()
    )
    assert rendered.query == (
        'select A { p_int16 } order by .p_int16 asc '
        'offset math::abs(<int16>$offset_0_0) '
        'limit math::abs(<int16>$limit_0_0)'
    )
    assert rendered.context == MappingProxyType({'limit_0_0': -2, 'offset_0_0': -4})
    result = client.query(rendered.query, **rendered.context)
    assert len(result) == 1
