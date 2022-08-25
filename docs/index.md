# EdgeQL Query Builder

`edgeql-qb` is a query builder for `EdgeQL` (EdgeDB Query Language) for python.
Query syntax should be familiar to `SQLAlchemy Core` users. 
On that point library not contains any dependencies, event on EdgeDB itself. 
It's a just renderer that knows nothing about your schema. 

## Installation

EdgeQL Query Builder can be installed with any package manager of your choice, but I'm guess that you are using `poetry`:

```shell
poetry add edgeql-qb
```

## Usage

Some useful imports that you may need to know:
```python
from edgedb import create_client  # or any other client that you want to use
from edgeql_qb import EdgeDBModel  # to declare EdgeDB type in python code 
from edgeql_qb import types  # use this for explicit type casts
```

Initialize client:
```python
client = create_client()  # it may asks you additional args depending on you configuration
```

Declare model that depends on EdgeDB Type:
```python
Movie = EdgeDBModel('Movie')  # additional args allow you to specify module and schema of a type.
```

### Select query
```python
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
print(select.query)
# select Movie { title, year, director: { first_name, last_name }, actors: { first_name, last_name } } filter .title = <str>$filter_1_0_0
print(select.context)
# {'filter_1_0_0': 'Blade Runner 2049'}
result = client.query_one(select.query, **select.context)
print(result.title)
# Blade Runner 2049
```

### Insert query
Subqueries are supported as well, but pay attention about limit argument for singular relationships. 
EdgeDB requires you to specify `limit 1` until you have not create unique constraint covering this condition.
When you pass python value without wrapper `limit(limit)` it will be rendered 
as context's variable and will pass to EdgeDB dynamically, which will cause an error on EdgeDB side.
Similar to how it works in SQLAlchemy's `unsafe_text` wrapper make this expression hardcoded into final query as is, 
without dynamic context.
```python
insert = Movie.insert.values(
    title='Blade Runner 2049',
    year=types.int16(2017),
    director=(
        Person.select()
        .where(Person.c.id == director_id)
        .limit1 
    ),
    actors=Person.insert.values(
        first_name='Harrison', 
        last_name='Ford',
    ),
).all()
print(insert.query)
# insert Movie { title := <str>$insert_1_0_0, year := <int16>$insert_1_1_0, director := (select Person filter .id = $filter_2_0_0 limit 1), actors := (insert Person { first_name := <str>$insert_2_0_0, last_name := <str>$insert_2_1_0 }) }
print(insert.context)
# {'insert_1_0_0': 'Blade Runner 2049', 'insert_1_1_0': 2017, 'filter_2_0_0': UUID('15e1155f-c94d-4ac0-bae6-f3d709b91a0e'), 'insert_2_0_0': 'Harrison', 'insert_2_1_0': 'Ford'}
```

### Delete query
You can delete all records `delete = Movie.delete.all()` or by condition:

```python
delete = Movie.delete.where(Movie.c.title == 'Blade Runner 2049').all()

print(delete.query)
# delete Movie filter .title = <str>$filter_1_0_0
print(delete.context)
# {'filter_1_0_0': 'Blade Runner 2049'}

client.query(delete.query, **delete.context)
```
