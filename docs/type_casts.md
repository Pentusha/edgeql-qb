# Type Casts
Python is blind regarding the difference between int16 and int64 so you need to
specify type to your literals with explicit casts:

```python
from edgeql_qb.types import int16

Movie.update.value(year=int16(2022)).where(Movie.c.id == movie_id).all()
```

## Implicit

Query builder helps you to omit explicit type casts for several
types which match between python and edgedb. interpreter allows to detect automatically.

| Python type        | EdgeQL Type         |
|--------------------|---------------------|
| str                | str                 |
| bool               | bool                |
| bytes              | bytes               |
| datetime (tz=None) | cal::local_datetime |
| datetime (with tz) | datetime            |
 | date               | cal::local_date     |
 | time               | cal::local_time     |
 | timedelta          | duration            |
 | Decimal            | Decimal             |


## Explicit

One type in Python can correspond to several types in EdgeDB.
In that case you need to use explicit type cast `A.c.som_int64_column == int64(value)`

| Python type | EdgeQL Types        |
|-------------|---------------------|
 | int         | int16, int32, int64 |
| float       | float32, float64    |
