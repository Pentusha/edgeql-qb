# Functions

Query builder supports function calls. You can use functions calls in any part of the query.
Functions may be nested and composed with other parts of the query.

```python
from edgeql_qb.func import math


Movie.select(math.len(Movie.c.title).label('title_len')).build()
```

## User-defined functions
You can call custom functions:
```
function exclamation(word: str) -> str
  using (word ++ '!');
```

```python
from edgeql_qb.func import Function

exclamation = Function('default', 'exclamation')
Movie.select(exclamation(Movie.c.title).label('title_exclamation')).build()
```

## Standard functions
Most of the functions from the standard library have already been added to the query builder.

### Math

```python
from edgeql_qb.func import math
```

    math::abs
    math::ceil
    math::floor
    math::ln
    math::mean
    math::stddev
    math::stddev_pop
    math::var
    math::var_pop

### Std

```python
from edgeql_qb.func import std
```
    std::assert_distinct
    std::assert_single
    std::assert_exists
    std::count
    std::sum
    std::all
    std::any
    std::enumerate
    std::min
    std::max
    std::len
    std::contains
    std::find
    std::uuid_generate_v1mc
    std::uuid_generate_v4
    std::random
    std::bit_and
    std::bit_or
    std::bit_xor
    std::bit_not
    std::bit_lshift
    std::bit_rshift
    std::to_bigint
    std::to_decimal
    std::to_int16
    std::to_int32
    std::to_int64
    std::to_float32
    std::to_float64
    std::range_get_lower
    std::range_get_upper
    std::range_is_inclusive_lower
    std::range_is_inclusive_upper
    std::range_is_empty
    std::range_unpack
    std::overlaps
    std::bytes_get_bit
    std::sequence_next
    std::sequence_reset
    std::array_join
    std::array_fill
    std::array_replace
    std::array_agg
    std::array_get
    std::array_unpack
    std::to_str
    std::to_datetime
    std::to_duration
    std::datetime_get
    std::duration_get
    std::datetime_truncate
    std::duration_truncate
    std::datetime_current
    std::datetime_of_transaction
    std::datetime_of_statement

### Sys

```python
from edgeql_qb.func import sys
```

    sys::get_version
    sys::get_version_as_str
    sys::get_current_database

### Cal

```python
from edgeql_qb.func import cal
```

    cal::local_datetime
    cal::local_date
    cal::local_time
    cal::relative_duration
    cal::date_duration
    cal::to_local_datetime
    cal::to_local_date
    cal::to_local_time
    cal::to_relative_duration
    cal::to_date_duration
    cal::time_get
    cal::date_get
    cal::duration_normalize_hours
    cal::duration_normalize_days
