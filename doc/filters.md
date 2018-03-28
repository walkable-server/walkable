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
;; column `person`.`foo`
;; operator `:nil?` which requires no arguments
[:person/foo [:nil?]]

;; column `person`.`name`
;; operator `:like` which requires one argument
[:person/name [:like "jon%"]]

;; column `person`.`id`
;; operator `:in` is supplied with three arguments
[:person/id [:in 10 11 12]]
;; finally argument list will be flattened, so this is valid, too:
[:person/id [:in #{10 11 12}]]
;; -> using a Clojure set is helpful in this case
;; so duplications are removed
```
### 1.2 Operators

Table of built-in operators

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


### 1.3 Operators' arguments

Arguments must be either string, number or column name (as namespaced
keyword) like this:

```clj
[:person/column-a [:< :person/column-b]]
```

### 2.2 Single condition, map style

```clj
;; Better looking, isn't it?
{:table/column [:sql-operator zero-or-more-arguments]}
```

Vector-style examples rewritten in map style:

```clj
;; column `person`.`foo`
;; operator `:nil?` which requires no arguments
{:person/foo [:nil?]}

;; column `person`.`name`
;; operator `:like` which requires one argument
{:person/name [:like "jon%"]}

;; column `person`.`id`
;; operator `:in` is supplied with three arguments
{:person/id [:in 10 11 12]}
{:person/id [:in #{10 11 12}]}
```

### 2.3 Map style, `:and` implied

Using multiple condition in a condition map implies an `:and` between
each condition:

```clj
;; filters
{:person/id  [:in #{10 11 12}]
 :person/yob [:< 1970]}
;; the same as:
[:and {:person/id [:in #{10 11 12}]}
      {:person/yob [:< 1970]}]
```

### 2.4 Use `:and` and `:or` to combine conditions of the same column

```clj
;; filters
[:or {:person/yob [:< 1970]}
     {:person/yob [:> 1980]}]
;; the same as:
{:person/yob [:or [:< 1970]
                  [:> 1980]]}
;; the same, vector style:
[:person/yob [:or [:< 1970]
                  [:> 1980]]]
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
;; two conditions combined with an `:and`
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

## 4 Extra-conditions vs supplied-conditions