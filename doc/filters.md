# Filters

Filters are Walkable's DSL to describe conditions in SQL's `WHERE`
clauses using pure Clojure data structure.

## Using filters

Any valid Walkable S-expression that evaluates to boolean (not just
some truthy values) can be used in filter.

### Using filters directly in queries

```clj
[{(:people/all {:filters [:or [:like :person/name "jon%"]
                             [:< :person/yob 1970]]})
  [:person/name :person/yob]]}
```

> This is called `supplied-conditions` in Walkable query builder
  engine's source code.

### Filters in `:extra-conditions` schema

You may want to enforce filters for specific idents/joins:

- For DRY reason: you don't want to retype the same filter set again
  and again.

- For security reason: once you define some constraints to limit what
  the clients can access for an ident or join, they're free to play
  with whatever left open to them.

```clj
;; schema
{:extra-conditions {:people/all [:= :person/hidden false]
;; any valid filter will work
                    :person/friends [:or
                                     [:like :person/name "jon%"]
                                     [:like :person/name "mary%"]]}}
```

## Joins in filters

You've seen filters used against columns of the current table. If you
have defined some joins, you can also put constraints on columns of
the joined tables, too.

```clj
;; find all people whose name starts with "jon" or whose friend's name
;; starts with "jon".
[{(:people/all {:filters [:or [:like :person/name "jon%"]
                          {:person/friend [:like :person/name "jon%"]}]})
  [:person/name :person/yob]}]
```

You can have many such joins in filters and combine them with other
expressions using `:and`/`:or`/`:not` however you like.