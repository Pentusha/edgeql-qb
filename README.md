# Scope
* Query builder for EdgeDB with syntax similar to the SQLAlchemy Core.
* The library does not contain any code to connect to the database or to execute queries.

# Description
* Project currently in pre-alpha status. It is not production-ready yet, and It may buggy and unstable as well.
* The project is not affiliated with the official developers of EdgeDB.
* This project only supports a small part of the EdgeDB syntax.
* The library does not introspect the database and will not check if you made a typo somewhere in a column name. What you write is what you get.
* Minimal required version of python is 3.10. Not sure if I'll ever do a backport.
* There is no external dependencies, even on EdgeDB itself.

# TODO
* Support array/json types
* Build a simple queries `select [1,2,3]`
* Optimize involution op. `-(-a) = a`, `not not a = a` etc.
* Optimize binary op. `a - a = -a + a = empty`
* Aggregations.
* Support functions calls.
* Implement `insert`/`update`/`delete` queries.
* Optional (Maybe) filters.
* Describe response schema.
* Support if/else statements.
* Validate query against declared schema.
* It would be cool to have mypy plugin.

# Usage examples
Many examples of queries are given in the [test](https://github.com/Pentusha/edgeql-qb/blob/master/tests/test_render.py) file.

```python
Movie = EdgeDBModel('Movie')


query = (
    Movie.select(
        Movie.c.title,
        Movie.c.year,
        Movie.c.director.select(
            Movie.c.director.first_name,
            Movie.c.director.last_name,
        ),
        Movie.c.actors.select(
            Movie.c.actors.first_name,
            Movie.c.actors.last_name,
        ),
    )
    .where(Movie.c.title == 'Blade Runner 2049')
    .all()
)

result = client.query(query.query, **query.context)
```
