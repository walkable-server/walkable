# Advanced syntax and features for schema

## 1. Syntactic sugars

### Multiple dispatch keys for `:idents`, `:extra-conditions`, `:joins` and `:cardinality`.

If two or more dispatch key share the same configuration, it's handy
to have them in the same entry. For example:

instead of:

```clj
;; schema
{:idents {:people/all "person"
          :my-friends "person"}}
```

this is shorter:

```clj
;; schema
{:idents {[:people/all :my-friends]
          "person"}}
```

This also applies to `:extra-conditions`, `:joins` and `:cardinality`.

## 2. Lambda form for `:extra-conditions`

Instead of specifying a fixed filters set for an ident or a join, you
can use a function that returns such filters set. The function accepts
`env` as its argument.

Please see [dev.clj](/dev/src/dev.clj) for examples.