# Queries

All queries are formed through chain method calls. The last method in the call
chain should be the `build()` method, which returns final query.

## Select

You can query whole objects `Movie.select().build()` that gives you
the following EdgeQL query: `select Movie`
You also can ask database for exact set of properties:
`Movie.select(Movie.c.title, Movie.c.year).build()` that would be rendered to `select Movie { title, year }`.

### Shapes
Nested shapes can be used to fetch linked objects and their properties.
Here we fetch all Movie objects and their directors.

```python
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
    ).order_by(Movie.c.actors.first_name, Movie.c.actors.last_name).limit(5)
).build()
```
<details>
  <summary>generated query</summary>

```
select Movie {
    title,
    year,
    director: {
        first_name,
        last_name
    },
    actors: {
        first_name,
        last_name
    }
    order by .first_name then .last_name
    limit <int64>$limit_0
}
{'limit_0': 5}
```
</details>

### Subqueries

If the query is used as a subquery, then the such query should not end with the `build()` method.

```python
Post.select(
    Post.c.title,
    Post.c.description,
    lastest_posts=Post.select().order_by(Post.c.created_at.desc()).limit(3),
)
```
<details>
  <summary>generated query</summary>

```
select Post { title, description, latest_posts := (select Post order by created_at desc limit $limit_0) }
```
</details>

### Expressions
Shapes can contain computed fields.
These are EdgeQL expressions that are computed on the fly during the execution of the query.
```python
Person.select(
    Person.c.name,
    (Person.c.weight / Person.c.height ** 2).label('bmi'),
).build()
```
<details>
  <summary>generated query</summary>

```
select Person { name, bmi := .weight / .height ^ $select_0 }
{'select_0': 2}
```
</details>

### Filtering
To filter the set of selected objects, use a `where` chained method,
which accepts either binary or unary expressions.
```python
Villain.select(Villain.c.id, Villain.c.name).where(Villain.c.name == 'Doc Ock').build()
```
<details>
  <summary>generated query</summary>

```
select Villain { id, name } filter .name = <str>$filter_0
{'filter_0': 'Doc Ock'}
```
</details>

You also may filter nested objects:
```python
Post.select(
    Post.c.title,
    Post.c.text,
    Post.c.comments(
        Comment.c.text,
    ).where(Comment.c.created_at >= created_after),
)
```

<details>
  <summary>generated query</summary>

```
select Post { title, text, comments: { text } filter .created_at >= <cal::local_datetime>$filter_0 }
```
</details>

### Ordering
You could pass any number of binary or unary expressions or even columns to `order_by` method:
```python
Movie.select().order_by(
    Movie.c.rating.desc(),
    Movie.c.year.desc(),
    Movie.c.title,
).build()
```
<details>
  <summary>generated query</summary>

```
select Movie order by .rating desc then .year desc then .title
```
</details>

### Pagination
You may build pagination on limit and offset expressions exactly like in SQL.

```python
top_250 = (
    Movie.select()
    .order_by(Movie.c.rating.desc())
    .limit(250)
    .offset(0)
    .build()
)
```

<details>
  <summary>generated query</summary>

```
select Movie order by .rating desc offset $offset_0 limit $limit_1
{'limit_1': 250, 'offset_0': 0}
```
</details>

In case you are using select or insert subquery for singular relationship then you need to know:

EdgeDB requires you to specify `limit 1` until you have not create unique constraint covering this condition.
When you pass python value without wrapper `unsafe_text(limit)` it will be rendered
as context's variable `limit_N` and will pass to EdgeDB dynamically, which will cause an error on EdgeDB side.
Similar to how it works in SQLAlchemy's `unsafe_text` wrapper make this expression hardcoded into final query as is,
without dynamic contexts.

Please note that `offset` by design producing not optional execution plan
and you have to avoid to use this keyword and method as far as you can.


## Insert

```python
Movie.insert.values(
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
```

<details>
  <summary>generated query</summary>

```
insert Movie {
    title := <str>$insert_0,
    year := <int16>$insert_1,
    director := (select Person filter .id = <uuid>$filter_2 limit 1),
    actors := (insert Person { first_name := <str>$insert_3, last_name := <str>$insert_4 })
}
{'insert_0': 'Blade Runner 2049', 'insert_1': 2017, 'filter_2': director_id, 'insert_3': 'Harrison', 'insert_4': 'Ford'}
```
</details>

For convenience, the `.limit1` property has been added, which is a shorthand for `limit(unsafe_text('1'))`.

### Conditional Insert (Upsert)
```python
(
    Movie
    .insert
    .values(
        slug='blade_runner_2049',
        title='Blade Runner 2049',
        usd_raised=int16(1000),
    )
    .unless_conflict(on=Movie.c.slug, else_=Movie.update.values(usd_raised=int16(1000))
    .build()
)
```

<details>
  <summary>generated query</summary>

```
insert Movie {
  title := <str>$insert_0,
  slug := <str>$insert_1,
  usd_raised := <int16>$insert_2,
}
unless conflict on .slug
else (update Movie set { usd_raised := <int16>$update_3 })
```
</details>

### Idempotent Insert
```python
Account.insert.values(username='System').unless_conflict().build()
```

<details>
  <summary>generated query</summary>

```
insert Account { username := <str>$insert_0 } unless conflict
```
</details>

### Select or insert
There are times when rather than an "upsert" you need to select an object or insert it, if it wasn't there.
Consider, for example, the functionality to create a new account or retrieve an existing one:
```python
(
    Account
    .select(
        Account.c.id,
        Account.c.username,
        Account.c.watchlist(
            Account.c.watchlist.title,
        ),
    )
    .select_from(
        Account
        .insert
        .values(username='Alice')
        .unless_conflict(Account.c.username, Account)
    )
    .build()
)
```
<details>
  <summary>generated query</summary>

```
select (
  insert Account {
    username := <str>$insert_0
  } unless conflict on .username else Account
) {
  id,
  username,
  watchlist: { title }
}
{'insert_0': 'Alice'}
```
</details>

## Update

```python
Movie.update.values(
    budget_usd=int16(185_000_000),
).where(Movie.c.title == 'Blade Runner 2049').build()
```

<details>
  <summary>generated query</summary>

```
update Movie filter .title = <str>$filter_0 set { budget_usd := <int16>$update_1 }
{'filter_0': 'Blade Runner 2049', 'update_1': 185000000}
```
</details>

## Delete

```python
Movie.delete.where(Movie.c.title == 'Blade Runner 2049').build()
```
<details>
  <summary>generated query</summary>

```
delete Movie filter .title = <str>$filter_0
{'filter_0': 'Blade Runner 2049'}
```
</details>

The `limit`, `offset` and `order_by` methods are supported as well.

This query will delete latest log entry:
```python
Log.delete.order_by(Log.c.created_at.desc()).limit1.build()
```
<details>
  <summary>generated query</summary>

```delete Log order by .created_at desc limit 1```
</details>


## Group

```python
Movie.group(Movie.c.title).by(Movie.c.year).build()
```
<details>
  <summary>generated query</summary>

```
group Movie { title } by .year
```
</details>

More complex example:
```python
decade = (Movie.c.year // 10).label('decade')
Movie.group().using(decade).by(decade).build()
```

<details>
  <summary>generated query</summary>

```
group Movie using decade := .year // $using_0 by decade
{'using_0': 10}
```
</details>

Using syntax also supports nested expressions:
```python
weight_kg = (Person.c.weight_grams / 1000).label('weight_kg')
height_cm = (Person.c.height_m * 100).label('height_cm')
bmi = (weight_kg / height_cm ** 2).label('bmi')
```

## With
All top-level EdgeQL statements (`select`, `insert`, `update`, and `delete`)
can be prefixed with a `with` block.
These blocks contain declarations of standalone expressions that can be used in your query.
Query builder provides a special method `with_` that allows you to set the values of these declarations.

```python
from edgeql_qb import EdgeDBModel
from edgeql_qb.operators import Alias
from edgeql_qb.types import int16

# please note that you may specify module for model,
# which would be used in every generated `with` statements.
Person = EdgeDBModel('Person', module='imdb')
Movie = EdgeDBModel('Movie', module='imdb')

actors = Person.insert.values(
    first_name='Harrison',
    last_name='Ford',
).label('actors')
director = Person.select().where(Person.c.id == director_id).limit1.label('director')
title = Alias('title').assign('Blade Runner 2049')
year = Alias('year').assign(int16(2017))
query = Movie.insert.with_(actors, director, title, year).values(
    title=title,
    year=year,
    director=director,
    actors=actors,
).build()
```

<details>
  <summary>generated query</summary>

```
with
    module imdb,
    actors := (with module imdb insert Person { first_name := <str>$insert_0, last_name := <str>$insert_1 }),
    director := (with module imdb select Person filter .id = $filter_2 limit 1),
    title := <str>$with_3,
    year := <int16>$with_4
insert Movie {
    title := title,
    year := year,
    director := director,
    actors := actors
}
{'insert_0': 'Harrison', 'insert_1': 'Ford', 'filter_2': director_id, 'with_3': 'Blade Runner 2049', 'with_4': 2017}
```
</details>
