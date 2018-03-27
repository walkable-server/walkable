# Filters

Filters are Walkable's DSL to describe conditions in SQL's `WHERE`
clauses using pure Clojure data structure. A set of filters contains
zero or more "conditions" combined together using `:and` or `:or`

Examples of filter sets:

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