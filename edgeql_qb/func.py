from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from edgeql_qb.operators import OperationsMixin


@dataclass(slots=True, frozen=True)
class Function:
    module: str
    name: str

    def __call__(self, *args: Any, **kwargs: Any) -> FuncInvocation:
        return FuncInvocation(func=self, args=args, arity=len(args))


@dataclass(slots=True, frozen=True)
class _Math:
    abs = Function(module='math', name='abs')
    ceil = Function(module='math', name='ceil')
    floor = Function(module='math', name='floor')
    ln = Function(module='math', name='log')
    mean = Function(module='math', name='mean')
    stddev = Function(module='math', name='stddev')
    stddev_pop = Function(module='math', name='stddev_pop')
    var = Function(module='math', name='var')
    var_pop = Function(module='math', name='var_pop')


@dataclass(slots=True, frozen=True)
class _Std:
    assert_distinct = Function(module='std', name='assert_distinct')
    assert_single = Function(module='std', name='assert_single')
    assert_exists = Function(module='std', name='assert_exists')
    count = Function(module='std', name='count')
    sum = Function(module='std', name='sum')
    all = Function(module='std', name='all')
    any = Function(module='std', name='any')
    enumerate = Function(module='std', name='enumerate')
    min = Function(module='std', name='min')
    max = Function(module='std', name='max')
    len = Function(module='std', name='len')
    contains = Function(module='std', name='contains')
    find = Function(module='std', name='find')
    uuid_generate_v1mc = Function(module='std', name='uuid_generate_v1mc')
    uuid_generate_v4 = Function(module='std', name='uuid_generate_v4')
    random = Function(module='std', name='random')
    bit_and = Function(module='std', name='bit_and')
    bit_or = Function(module='std', name='bit_or')
    bit_xor = Function(module='std', name='bit_xor')
    bit_not = Function(module='std', name='bit_not')
    bit_lshift = Function(module='std', name='bit_lshift')
    bit_rshift = Function(module='std', name='bit_rshift')
    to_bigint = Function(module='std', name='to_bigint')
    to_decimal = Function(module='std', name='to_decimal')
    to_int16 = Function(module='std', name='to_int16')
    to_int32 = Function(module='std', name='to_int32')
    to_int64 = Function(module='std', name='to_int64')
    to_float32 = Function(module='std', name='to_float32')
    to_float64 = Function(module='std', name='to_float64')
    range_get_lower = Function(module='std', name='range_get_lower')
    range_get_upper = Function(module='std', name='range_get_upper')
    range_is_inclusive_lower = Function(module='std', name='range_is_inclusive_lower')
    range_is_inclusive_upper = Function(module='std', name='range_is_inclusive_upper')
    range_is_empty = Function(module='std', name='range_is_empty')
    range_unpack = Function(module='std', name='range_unpack')
    overlaps = Function(module='std', name='overlaps')
    bytes_get_bit = Function(module='std', name='bytes_get_bit')
    sequence_next = Function(module='std', name='sequence_next')
    sequence_reset = Function(module='std', name='sequence_reset')
    array_join = Function(module='std', name='array_join')
    array_fill = Function(module='std', name='array_fill')
    array_replace = Function(module='std', name='array_replace')
    array_agg = Function(module='std', name='array_agg')
    array_get = Function(module='std', name='array_get')
    array_unpack = Function(module='std', name='array_unpack')
    to_str = Function(module='std', name='to_str')
    to_datetime = Function(module='std', name='to_datetime')
    to_duration = Function(module='std', name='to_duration')
    datetime_get = Function(module='std', name='datetime_get')
    duration_get = Function(module='std', name='duration_get')
    datetime_truncate = Function(module='std', name='datetime_truncate')
    duration_truncate = Function(module='std', name='duration_truncate')
    datetime_current = Function(module='std', name='datetime_current')
    datetime_of_transaction = Function(module='std', name='datetime_of_transaction')
    datetime_of_statement = Function(module='std', name='datetime_of_statement')


@dataclass(slots=True, frozen=True)
class _Sys:
    get_version = Function(module='sys', name='get_version')
    get_version_as_str = Function(module='sys', name='get_version_as_str')
    get_current_database = Function(module='sys', name='get_current_database')


@dataclass(slots=True, frozen=True)
class _Cal:
    local_datetime = Function(module='std', name='local_datetime')
    local_date = Function(module='std', name='local_date')
    local_time = Function(module='std', name='local_time')
    relative_duration = Function(module='std', name='relative_duration')
    date_duration = Function(module='std', name='date_duration')
    to_local_datetime = Function(module='std', name='to_local_datetime')
    to_local_date = Function(module='std', name='to_local_date')
    to_local_time = Function(module='std', name='to_local_time')
    to_relative_duration = Function(module='std', name='to_relative_duration')
    to_date_duration = Function(module='std', name='to_date_duration')
    time_get = Function(module='std', name='time_get')
    date_get = Function(module='std', name='date_get')
    duration_normalize_hours = Function(module='std', name='duration_normalize_hours')
    duration_normalize_days = Function(module='std', name='duration_normalize_days')


math = _Math()
std = _Std()
sys = _Sys()
cal = _Cal()


@dataclass(slots=True, frozen=True)
class FuncInvocation(OperationsMixin):
    func: Function
    args: tuple[Any, ...]
    arity: int
