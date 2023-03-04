[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/edgeql-qb)](https://badge.fury.io/py/edgeql-qb)
[![PyPI version](https://badge.fury.io/py/edgeql-qb.svg)](https://badge.fury.io/py/edgeql-qb)
[![Tests](https://github.com/Pentusha/edgeql-qb/actions/workflows/tests.yml/badge.svg)](https://github.com/Pentusha/edgeql-qb/actions/workflows/tests.yml)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=Pentusha_edgeql-qb&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=Pentusha_edgeql-qb)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=Pentusha_edgeql-qb&metric=coverage)](https://sonarcloud.io/summary/new_code?id=Pentusha_edgeql-qb)

# EdgeQL Query Builder

Query builder for EdgeDB

## Description
* This project is currently in pre-alpha status, meaning that it is not yet production-ready and may be buggy and unstable.
* Please note that this project is not affiliated with the official developers of EdgeDB.
* Additionally, it is important to know that this project only supports a small part of the EdgeDB syntax.
* The library does not include any code for connecting to the database or executing queries.
* It also does not provide database introspection, so it will not check for typos in column names. What you write is what you get.
* Python version 3.10 or higher is required to use this library, and there are currently no plans for backporting to earlier versions.
* There are no external dependencies, including EdgeDB itself.

# Usage examples
Many examples of queries are given in the [documentation](https://pentusha.github.io/edgeql-qb/queries) directory.

```python
from edgeql_qb import EdgeDBModel
from edgeql_qb.types import int16
from edgedb.blocking_client import create_client


client = create_client()
Movie = EdgeDBModel('Movie')
Person = EdgeDBModel('Person')

insert = Movie.insert.values(
    title='Blade Runner 2049',
    year=int16(2017),
    director=(
        Person.select()
        .where(Person.c.id == director_id)
        .limit1
    ),
    actors=Person.insert.values(
        first_name='Harrison',
        last_name='Ford',
    ),
).build()


select = (
    Movie.select(
        Movie.c.title,
        Movie.c.year,
        Movie.c.director(
            Movie.c.director.first_name,
            Movie.c.director.last_name,
        ),
        Movie.c.actors(
            Movie.c.actors.first_name,
            Movie.c.actors.last_name,
        ),
    )
    .where(Movie.c.title == 'Blade Runner 2049')
    .build()
)

delete = Movie.delete.where(Movie.c.title == 'Blade Runner 2049').build()

decade = (Movie.c.year // 10).label('decade')
group = Movie.group().using(decade).by(decade).build()

client.query(insert.query, **insert.context)
result = client.query(select.query, **select.context)

movies_by_decade = client.query(group.query, **group.context)

client.query(delete.query, **delete.context)
```

## Status
- Queries:
  - [x] select
    - [x] [nested shapes](https://www.edgedb.com/tutorial/nested-structures/shapes)
      - [x] filters for nested shapes
      - [x] order by for nested shapes
      - [x] limit/offset for nested shapes
      - [x] aggregations for nested shapes
    - [x] function calls
    - [x] computed fields
    - [x] filters
      - [x] filter by nested paths
    - [x] limit & offset
    - [x] order by
    - [ ] [backlinks](https://www.edgedb.com/docs/edgeql/paths#backlinks)
    - [x] [subqueries](https://www.edgedb.com/tutorial/nested-structures/shapes/subqueries)
    - [ ] [polymorphic fields](https://www.edgedb.com/tutorial/nested-structures/polymorphism)
    - [ ] [link properties](https://www.edgedb.com/docs/edgeql/paths#link-properties) (@notation)
    - [ ] [detached](https://github.com/edgedb/edgedb/blob/master/docs/reference/edgeql/with.rst)
  - [x] group
    - [x] columns
    - [x] using
    - [x] by
    - [x] function calls
  - [x] update
    - [x] function calls
    - [x] nested queries
  - [x] delete
    - [x] delete without filters
    - [x] function calls
    - [x] limit & offset
    - [x] order by
  - [x] insert
    - [x] [nested inserts](https://www.edgedb.com/docs/edgeql/insert#nested-inserts)
    - [X] [conditional inserts](https://www.edgedb.com/tutorial/data-mutations/upsert/conditional-inserts)
    - [x] [idempotent insert](https://www.edgedb.com/tutorial/data-mutations/upsert/idempotent-insert)
    - [x] [select-or-insert](https://www.edgedb.com/tutorial/data-mutations/upsert/select-or-insert)
  - [x] function calls
    - [x] positional arguments
    - [ ] keyword arguments
  - [x] [with block](https://www.edgedb.com/tutorial/nested-structures/shapes/with-block)
    - [x] with literal
    - [x] with subquery
    - [x] with module + closure
    - [ ] with x := subquery select x
    - [ ] with x := subquery group x
    - [ ] with x := subquery update x
    - [ ] with x := Type.column
  - [ ] if statements
  - [ ] [globals](https://www.edgedb.com/docs/datamodel/globals#globals)
  - [ ] [for statements](https://www.edgedb.com/docs/edgeql/paths#link-properties)
    - [ ] union statements
  - [ ] queries without models, like select [1,2,3]
- Types:
  - [x] type casts
  - [ ] cal::date_duration
  - [ ] cal::relative_duration
  - [ ] std::array
  - [ ] std::json
  - [ ] std::range
  - [ ] std::set
  - [ ] std::tuple
  - [x] cal::local_date
  - [x] cal::local_date
  - [x] cal::local_datetime
  - [x] cal::local_time
  - [x] std::bigint
  - [x] std::bool
  - [x] std::bytes
  - [x] std::datetime
  - [x] std::decimal
  - [x] std::duration
  - [x] std::float32
  - [x] std::float64
  - [x] std::int16
  - [x] std::int32
  - [x] std::int64
  - [x] std::str
  - [x] std::uuid

- Functions
  - [x] cal
  - [x] math
  - [x] std
  - [x] sys
