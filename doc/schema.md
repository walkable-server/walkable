# Walkable Schema

As you can see in **Usage** guide, we need to provide
`sqb/compile-schema` a map describing the schema. You've got some idea
about how such a schema looks like as you glanced the example in
**Overview** section. Now let's walk through each key of the schema
map in details.

> Notes:
> - SQL snippets have been simplified for explanation purpose
> - Backticks are used as quote marks
> - For clarity, part of the schema may not be included

## :idents

Idents are the root of all queries. From an SQL dbms perspective, you
must start your graph query from somewhere, and it's a table.

### Keyword idents

Keyword idents can be defined as simple as:

```clj
;; schema
{:idents {:people/all "person"}}
```

so queries like:

```clj
[:people/all [:person/id :person/name]]
```

will result in an SQL query like:

```sql
SELECT `id`, `name` FROM `person`
```

### Vector idents

These are idents whose key implies some condition. Instead of
providing just the table, you provide the column (as a namespaced
keyword) whose value match the ident argument found in the ident key.

For example, the following vector ident:

```clj
;; dispatch-key: :person/by-id
;; ident arguments: 1
[:person/by-id 1]
```

will require a schema like:

```clj
;; schema
{:idents {:person/by-id :person/id}}
```

so queries like:

```clj
;; query
[[:person/by-id 1] [:person/id :person/name]]
```

will result in an SQL query like:

```sql
SELECT `id`, `name` FROM `person` WHERE `person`.`id` = 1
```

## :joins

Each join schema describes the "path" from the source table (of the
source entity) to the target table (and optionally the join table).

Let's see some examples.

### Example 1:

Assume table `cow` contains:

```
|----+-------|
| id | color |
|----+-------|
| 10 | white |
| 20 | brown |
|----+-------|
```

and table `farmer` has:

```
|----+------+--------|
| id | name | cow_id |
|----+------+--------|
|  1 | jon  |     10 |
|  2 | mary |     20 |
|----+------+--------|
```

and you want to get a farmer along with their cow using the query:

```clj
[{[:farmer/by-id 1]} [:farmer/name {:farmer/cow [:cow/id :cow/color]}]]
```

> For the join `:farmer/cow`, table `farmer` is the source and table
  `cow` is the target.

then you must define the join "path" like this:

```clj
;; schema
{:joins {:farmer/cow [:farmer/cow-id :cow/id]}}
```

the above join path says: start with the value of column
`farmer.cow_id` (the join column) then find the correspondent in the
column `cow.id`.

todo: generated SQL queries here.
tode: sample result.

### Example 2: Join column living in target table

wip

### Example 3: A join involving a join table

Assume the following tables:

source table  `person`:

```
|----+------|
| id | name |
|----+------|
|  1 | jon  |
|  2 | mary |
|----+------|
```

target table `pet`:

```
|----+--------|
| id | name   |
|----+--------|
| 10 | kitty  |
| 11 | maggie |
| 20 | ginger |
|----+--------|
```

join table `person_pet`:

```
|-----------+--------+---------------|
| person_id | pet_id | adoption_year |
|-----------+--------+---------------|
|         1 |     10 |          2010 |
|         1 |     11 |          2011 |
|         2 |     20 |          2010 |
|-----------+--------+---------------|
```

you may query for a person and their pets along with their adoption year

```clj
[{[:person/by-id 1] [:person/name {:person/pets [:pet/name :person-pet/adoption-year]}]}]
```

then the schema for the join is as simple as:

```clj
;; schema
{:joins {:person/pets [:person/id :person-pet/person-id
                       :person-pet/pet-id :pet/id]}}
```

todo: generated SQL queries
todo: sample result

## :reversed-joins

join specs are lengthy, to avoid typing the reversed path again
also it helps with semantics.

todo: example 1 and example 3 from `:join` section

## :columns

list of available columns must be provided beforehand.

- pre-compute strings
- keywords not in the column set will be ignored

## :cardinality

Idents and joins can have cardinality of either "many" (which is
default) or "one".

## :extra-conditions

## :quote-marks

Different SQL databases use different strings to denote quotation. For
instance, MySQL use a pair of backticks:

```sql
SELECT * FROM `table`
```

while PostgreSQL use quotation marks:

```sql
SELECT * FROM "table"
```

You need to provide the quote-marks as a vector of two strings

```clj
;; for mysql
{:quote-marks ["`", "`"]}
```

or
```clj
;; for postgres
{:quote-marks ["\"", "\""]}
```

For convenience, you can use the predefined vars instead:

```clj
;; for mysql
{:quote-marks sqb/backticks}
;; or postgresql
{:quote-marks sqb/quotation-marks}
```

The default quote-marks are the backticks, which work for mysql and
sqlite.

## :sqlite-union

Set it to `true` if you're using SQLite, otherwise ignore
it. Basically SQLite don't allow to union queries directly.
With other SQL DBMS, the union query can be something like:

```sql
SELECT a, b FROM foo WHERE ...
UNION ALL
SELECT a, b FROM foo WHERE ...
```

SQLite will raise a syntax error exception for such queries. Instead,
a valid query for SQLite must be something like:

```sql
SELECT * FROM (SELECT a, b FROM foo WHERE ...)
UNION ALL
SELECT * FROM (SELECT a, b FROM foo WHERE ...)
```

`{:sqlite-union true}` is for enforcing just that.

## :required-columns
## :pseudo-columns (Experimental)

## Syntactic sugars

### Multiple dispatch keys for the same configuration