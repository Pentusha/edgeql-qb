# Operations

Please note that the exponentiation operation differs between `python`'s `**` and `^` in `EdgeDB`. 
Query builder is using `python`'s `**` binary operator.
Any operation may be performed with `.op(operation)` helper. 
Some operations can not be expressed without `op` helper because python and EdgeQL 
got different set of operations. 

For example:
```
Person.c.first_name.op('++')(' ').op('++')(Person.c.last_name)
```
`.first_name ++ ' ' ++ .last_name` could not be expressed with python because 
python unlikely EdgeQl uses `+` for both addition and concatenation.  

## Implemented

| EdgeQL   | Python                      | Precedence     | Associative |
|----------|-----------------------------|----------------|-------------|
| `^`      | `__pow__` - `**`            | 9              | right       |
| `*`      | `__mul__` - `*`             | 8              | left        |
| `/`      | `__truediv__`               | 8              | left        |
| `//`     | `__floordiv__`              | 8              | left        |
| `%`      | `__mod__`                   | 8              | left        |
| `+`      | `__add__` - binary `+`      | 7              | left        |
| `-`      | `__sub__` - binary '-'      | 7              | left        |
| `++`     | `.op('++')` - concatenation | 7              | left        |
| `>`      | `__gt__`                    | 6              | left        |
| `<`      | `__lt__`                    | 6              | left        |
| `>=`     | `__ge__`                    | 6              | left        |
| `<=`     | `__le__`                    | 6              | left        |
| `like`   | `.like()`                   | 5              | left        |
| `ilike`  | `.ilike()`                  | 5              | left        |
| `=`      | `__eq__`                    | 4              | left        |
| `!=`     | `__ne__`                    | 4              | left        |
| `not`    | `__neg__` - unary `~`       | 3              | left        |
| `exists` | `.exists()`                 | 3              | left        |
| `and`    | `__and__` - `&`             | 2              | left        |
| `or`     | `__or__` - `&#124;`         | 1              | left        |
| `:=`     | `.label('left_side')`       | 0              | right*      |

Associativity for `:=` operator is marked with asterisk because it is 
right associative operator, but it is forbidden there syntactically.

Most likely, further description may be needed only if you decide to delve into the implementation.

## Associativity and Precedence
These two parameters determine the rules for placing parentheses in expressions.

Operation is left associative when you can omit left parentheses: `(a $ b) $ c == a $ b $ c`, where `$` is various binary operator.

Operation is right associative when you can omit right parentheses: `a $ (b $ c) = a $ b $ c`.

How to check it:
```
select 2/(3/4) = 2/3/4; -- false
select (2/3)/4 = 2/3/4; -- true, so / is left associative
select (2^3)^4 = 2^3^4; -- false
select 2^(3^4) = 2^3^4; -- true, so ^ is right associative
```

## Simplifications
The `edgeql-qb` implements few arithmetic simplifications.

* a + -b = a - +b = a - b
* a - -b = a + b
