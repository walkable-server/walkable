# Walkable Schema

As you can see in **Usage** guide, we need to provide
`sqb/compile-schema` a map describing the schema. You've got some idea
about how such a schema looks like as you glanced the example in
**Overview** section. Now let's walk through each key of the schema
map in details.

> Note about SQL snippets below:
> - they have been simplified for explanation purpose
> - they use backticks as quote marks

## :idents

Idents are the root of all queries. From an SQL dbms perspective, you
must start from a table.

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

describes the "path" from the source

## :reversed-joins

join specs are lengthy, to avoid typing the reversed path again
also it helps with semantics.

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