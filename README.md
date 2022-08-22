# EdgeQL Query Builder

## Scope
* Syntax similar to the SQLAlchemy Core.
* The library does not contain any code to connect to the database or to execute queries.

## Description
* Project currently in pre-alpha status. It is not production-ready yet, and It may buggy and unstable as well.
* The project is not affiliated with the official developers of EdgeDB.
* This project only supports a small part of the EdgeDB syntax.
* The library does not introspect the database and will not check if you made a typo somewhere in a column name. What you write is what you get.
* Minimal required version of python is 3.10. Not sure if I'll ever do a backport.
* There is no external dependencies, even on EdgeDB itself.

## Features
* Supports building `select`, `group`, `update`, `delete` and `insert` queries.
* Supports filtering and ordering.
* Support limit and offset.
* Supports nested scopes.
* Supports functions calls.

# Usage examples
Many examples of queries are given in the [tests](https://github.com/Pentusha/edgeql-qb/tree/master/tests/test_renderer) directory.

```python
from edgeql_qb import EdgeDBModel
from edgeql_qb.types import int16, unsafe_text
from edgedb.blocking_client import Client, create_client


client = create_client()
Movie = EdgeDBModel('Movie')
Person = EdgeDBModel('Person')

insert = Movie.insert.values(
    title='Blade Runner 2049',
    year=int16(2017),
    director=(
        Person.select()
        .where(Person.c.id == director_id)
        .limit(unsafe_text('1')) 
    ),
    actors=Person.insert.values(
        first_name='Harrison', 
        last_name='Ford',
    ),
).all()


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
    .all()
)

delete = Movie.delete.where(Movie.c.title == 'Blade Runner 2049').all()

decade = (Movie.c.year // 10).label('decade')
group = Movie.group().using(decade).by(decade).all()

client.query(insert.query, **insert.context)
result = client.query(select.query, **select.context)

movies_by_decade = client.query(group.query, group.context)

client.query(delete.query, **delete.context)
```

# TODO
* `with` clause.
* `upsert` queries.
* Aggregations.
* Support array/json types
* Build a simple queries `select [1,2,3]`
* Optional (Maybe) filters.
* Describe response schema.
* Support if/else statements.
* Validate query against declared schema.
* It would be cool to have mypy plugin.
* Optimize involution op. `-(-a) = a`, `not not a = a` etc.
* Optimize binary op. `a - a = -a + a = empty`
