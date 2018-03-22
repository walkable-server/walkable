# Walkable Schema

As you can see in **Usage** guide, we need to provide
`sqb/compile-schema` a map describing the schema. You've got some idea
about how such a schema looks like as you glanced the example in
**Overview** section. Now let's walk through each key of the schema
map in details.

## :idents

root of all queries

plain idents

idents whose key implies some condition

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

For convenience, you can use the predefined vars `sqb/backticks` or
`sqb/quotation-marks` instead.

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
## :formulas (Experimental)

## Syntactic sugars

### Multiple dispatch keys for the same configuration