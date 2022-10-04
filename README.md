[![Python 3.10+](https://img.shields.io/badge/python-3.10-green.svg)](https://www.python.org/downloads/release/python-3100/)
[![Tests](https://github.com/Pentusha/edgeql-qb/actions/workflows/tests.yml/badge.svg)](https://github.com/Pentusha/edgeql-qb/actions/workflows/tests.yml)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=Pentusha_edgeql-qb&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=Pentusha_edgeql-qb)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=Pentusha_edgeql-qb&metric=coverage)](https://sonarcloud.io/summary/new_code?id=Pentusha_edgeql-qb)

# EdgeQL Query Builder

Query builder for EdgeDB

## Description
* Project currently in pre-alpha status. It is not production-ready yet, and It may be buggy and unstable as well.
* The project is not affiliated with the official developers of EdgeDB.
* This project only supports a small part of the EdgeDB syntax.
* The library does not contain any code to connect to the database or to execute queries.
* The library does not introspect the database and will not check if you made a typo somewhere in a column name. What you write is what you get.
* Minimal required version of python is 3.10. Not sure if I'll ever do a backport.
* There is no external dependencies, even on EdgeDB itself.

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
