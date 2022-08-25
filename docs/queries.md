# Queries


## Select

You can query whole objects `Movie.select().all()` that gives you 
the following EdgeQL query: `select Movie`
You also can ask database for exact set of properties: 
`Movie.select(Movie.c.title, Movie.c.year).all()` that would be rendered to `select Movie { title, year }`.

### Shapes
Nested shapes can be used to fetch linked objects and their properties. 
Here we fetch all Movie objects and their directors.

```
Movie.select(
    Movie.c.title,
    Movie.c.year,
    Movie.c.director(
        Movie.c.director.first_name,
        Movie.c.director.last_name,
    ),
).all()
```
Will produce:

```
select Movie { title, year, { first_name, last_name } }
```

### Expressions
Shapes can contain computed fields. 
These are EdgeQL expressions that are computed on the fly during the execution of the query. 
```python
Person.select(
    Person.c.name,
    (Person.c.weight / Person.c.height ** 2).label('bmi'),
).all()
```
Will produce
```
select Person { name, bmi := .weight / .height ^ $select_1_0_0 }
{'select_1_0_0': 2}
```

### Filtering
To filter the set of selected objects, use a `where` chained method, 
which accepts either binary or unary expressions.
```python
Villain.select(Villain.c.id, Villain.c.name).where(Villain.c.name == 'Doc Ock').all()
```
Will produce
```python
select Villain { id, name } filter .name = <str>$filter_1_0_0
{'filter_1_0_0': 'Doc Ock'}
```


### Ordering
You could pass any number of binary or unary expressions or even columns to `order_by` method:
```
Movie.select().order_by(
    Movie.c.rating.desc(),
    Movie.c.year.desc(),
    Movie.c.title,
).all()
```


### Pagination
You may build pagination on limit and offset expressions exactly like in SQL.

```python
top_250 = (
    Movie.select()
    .order_by(Movie.c.rating.desc())
    .limit(250)
    .offset(0)
    .all()
)
```
yield:
```
select Movie order by .rating desc limit $limit_1 offset $offset_1
{'limit_1': 250, 'offset_1': 0}
```

In case you are using select or insert subquery for singular relationship then you need to know:

EdgeDB requires you to specify `limit 1` until you have not create unique constraint covering this condition.
When you pass python value without wrapper `unsafe_text(limit)` it will be rendered
as context's variable `limit_N` and will pass to EdgeDB dynamically, which will cause an error on EdgeDB side.
Similar to how it works in SQLAlchemy's `unsafe_text` wrapper make this expression hardcoded into final query as is, 
without dynamic contexts.

Please note that `offset` by design producing not optional execution plan
and you have to avoid to use this keyword and method as far as you can.


# Insert

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
).all()
```

For convenience, the `.limit1` property has been added, which is a shorthand for `limit(unsafe_text('1'))`.

# Update

```python
Movie.update.values(
    budget_usd=int16(185_000_000),
).where(Movie.c.title == 'Blade Runner 2049').all()
```
will produce:
```
update Movie filter .title = <str>$filter_1_0_0 set { budget_usd := <int16>$update_1_0_0 }
{'filter_1_0_0': 'Blade Runner 2049', 'update_1_0_0': 185000000}
```

# Delete

```python
Movie.delete.where(Movie.c.title == 'Blade Runner 2049').all()
```
will produce:
```
delete Movie filter .title = <str>$filter_1_0_0
{'filter_1_0_0': 'Blade Runner 2049'}
```

# Group
```python
Movie.group(Movie.c.title).by(Movie.c.year).all()
```
will produce:
```
group Movie { title } by .year
```

More complex example:
```python
decade = (Movie.c.year // 10).label('decade')
Movie.group().using(decade).by(decade).all()
```
will produce:
```
group Movie using decade := .year // $using_0_0_0 by decade
{'using_0_0_0': 10}
```

Using syntax also supports nested expressions.
```python
weight_kg = (Person.c.weight_grams / 1000).label('weight_kg')
height_cm = (Person.c.height_m * 100).label('height_cm')
bmi = (weight_kg / height_cm ** 2).label('bmi')
```
