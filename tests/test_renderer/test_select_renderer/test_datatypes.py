from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from edgeql_qb import EdgeDBModel
from edgeql_qb.expression import Column
from edgeql_qb.frozendict import FrozenDict
from edgeql_qb.types import (
    GenericHolder,
    bigint,
    float32,
    float64,
    int32,
    int64,
)

_dt = datetime(2022, 5, 26, 0, 0, 0)  # noqa: DTZ001
A = EdgeDBModel('A')
_TYPES = (
    ('bool_exp', A.c.p_bool, False, 'bool'),
    ('str_exp', A.c.p_str, 'Hello', 'str'),
    ('datetime_exp', A.c.p_local_datetime, _dt.replace(tzinfo=timezone.utc), 'datetime'),
    ('local_datetime_exp', A.c.p_local_dattime, _dt, 'cal::local_datetime'),
    ('local_date_exp', A.c.p_local_date, _dt.date(), 'cal::local_date'),
    ('local_time_exp', A.c.p_local_time, _dt.time(), 'cal::local_time'),
    ('duration_exp', A.c.p_duration, timedelta(), 'duration'),
    ('int32_exp', A.c.p_int32, int32(1), 'int32'),
    ('int64_exp', A.c.p_int64, int64(1), 'int64'),
    ('bigint_exp', A.c.p_bigint, bigint(1), 'bigint'),
    ('float32_exp', A.c.p_float32, float32(1), 'float32'),
    ('float64_exp', A.c.p_float64, float64(1), 'float64'),
    ('decimal_exp', A.c.p_decimal, Decimal(1), 'decimal'),
    ('bytes_exp', A.c.p_bytes, b'Hello', 'bytes'),
)


@pytest.mark.parametrize(['label', 'column', 'value', 'expected_type'], _TYPES)
def test_select_datatypes(
    label: str,
    column: Column,
    value: Any,
    expected_type: str,
) -> None:
    rendered = A.select((column != value).label(label)).build()
    assert rendered.query == (
        f'select A {{ {label} := .{column.column_name} != <{expected_type}>$select_0 }}'
    )
    assert rendered.context == FrozenDict(
        select_0=isinstance(value, GenericHolder) and value.value or value,
    )
