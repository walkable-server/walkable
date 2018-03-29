# Filters

Filters are Walkable's DSL to describe conditions in SQL's `WHERE`
clauses using pure Clojure data structure. A set of filters contains
zero or more "conditions" combined together using `:and` or `:or`

## 1 Single condition, vector style

### 1.1 Structure of a condition

```clj
;; structure
[:table/column [:sql-operator zero-or-more-arguments]]
```

Examples:

```clj
;; condition for column `person`.`foo`
;; operator `:nil?` which requires no arguments
[:person/foo [:nil?]]

;; condition for column `person`.`name`
;; operator `:like` which requires one argument
[:person/name [:like "jon%"]]

;; condition for column `person`.`id`
;; operator `:in` is supplied with three arguments
[:person/id [:in 10 11 12]]
;; finally argument list will be flattened, so this is valid, too:
[:person/id [:in #{10 11 12}]]
;; -> using a Clojure set is helpful in this case
;; so duplications are removed
```
### 1.2 Operators

Table of built-in operators

```txt
|--------------+---------------------|
| operator     | SQL equivalent      |
|--------------+---------------------|
| :nil?        | IS NULL             |
| :not-nil?    | IS NOT              |
| :=           | =                   |
| :<           | <                   |
| :>           | >                   |
| :<=          | <=                  |
| :>=          | >=                  |
| :<>          | <>                  |
| :like        | LIKE                |
| :not=        | !=                  |
| :not-like    | NOT LIKE            |
| :between     | BETWEEN ? AND ?     |
| :not-between | NOT BETWEEN ? AND ? |
| :in          | IN (?, ?, ...)      |
| :not-in      | NOT IN (?, ?, ...)  |
|--------------+---------------------|
```

### 1.3 Operators' arguments

Arguments must be either string, number or column name (as namespaced
keyword) like this:

```clj
;; condition
[:person/column-a [:< :person/column-b]]
```

## 2 Conditions, map style

### 2.1 Single condition, map style

```clj
;; Better looking, isn't it?
;; condition
{:table/column [:sql-operator zero-or-more-arguments]}
```

Vector-style examples rewritten in map style:

```clj
;; condition for column `person`.`foo`
;; operator `:nil?` which requires no arguments
{:person/foo [:nil?]}

;; condition for column `person`.`name`
;; operator `:like` which requires one argument
{:person/name [:like "jon%"]}

;; condition for column `person`.`id`
;; operator `:in` is supplied with three arguments
{:person/id [:in 10 11 12]}
{:person/id [:in #{10 11 12}]}
```

### 2.2 Multiple conditions, map style

Using multiple conditions in a condition map implies an `:and` between
each condition:

```clj
;; filters
{:person/id  [:in #{10 11 12}]
 :person/yob [:< 1970]}
;; the same as:
[:and {:person/id [:in #{10 11 12}]}
      {:person/yob [:< 1970]}]
```

## 3. Filter sets:

```clj
;; a single condition
{:person/id [:< 10]}
```

```sql
SELECT ... WHERE `person`.`id` < 10
```

```clj
;; filter set of two conditions combined with an `:and`
[:and {:person/id [:in #{10 11 12}]}
      {:person/yob [:< 1970]}]
;; `:and` is implied. The above is the same as:
[{:person/id [:in #{10 11 12}]}
 {:person/yob [:< 1970]}]
```

```sql
SELECT ...
WHERE `person`.`id` IN (10, 11, 12)
  AND `person`.`yob` < 1970
```

```clj
;; `:or` is a valid combinator
[:or {:person/name [:like "jon%"]}
     {:person/name [:like "jan%"]}]
```

```sql
SELECT ...
WHERE `person`.`name` LIKE "jon%"
   OR `person`.`name` LIKE "jan%"
```

you can have many conditions nested in however complex combination of
`:and` and `:or`:

```clj
[:or [:and condition-1 condition-2]
     [:and condition-3
          [:or condition-4
               [:and condition-5 condition-6]]]]
```

Use `:and` and `:or` to combine conditions of the same column

```clj
;; filter set with two conditions for `person`.`yob`
[:or {:person/yob [:< 1970]}
     {:person/yob [:> 1980]}]
;; the same as:
{:person/yob [:or [:< 1970]
                  [:> 1980]]}
;; the same, vector style:
[:person/yob [:or [:< 1970]
                  [:> 1980]]]
```

## 4 Using filters

### 4.1 Using directly in queries

```clj
[(:people/all {:filters [:or {:person/name [:like "jon%"]}
                             {:person/yob [:< 1970]}]})
 [:person/name :person/yob]]
```

> This is called `supplied-conditions` in Walkable query builder
  engine's source code.

### 4.2 In `:extra-conditions` schema

You may want to enforce filters for specific idents/joins:

- For DRY reason: you don't want to retype the same filter set again
  and again.

- For security reason: once you define some constraints to limit what
  the clients can access for an ident or join, they're free to play
  with whatever left open to them.

```clj
;; schema
{:extra-conditions {:people/all {:person/hidden [:= false]}
;; any valid filter set will work
                    :person/friends {:person/name
                                     [:or
                                      [:like "jon%"]
                                      [:like "mary%"]]}}}
```

## 5 Define your own operator

There are two `multimethod`s in `walkable.sql-query-builder.filters`
namespace you must implement in order to define your own operator.

```clj
(require '[walkable.sql-query-builder.filters :as sqbf])
```
