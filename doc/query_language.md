# Graph Query Syntax

Walkable uses a graph query language to describe what data you want
from your SQL DBMS. The query language was inspired by Datomic's [Pull
API](https://docs.datomic.com/on-prem/pull.html) and
[Graphql](https://http://graphql.org/) and first introduced in
[om.next](https://github.com/omcljs/om/wiki/Documentation-(om.next)). However,
you don't have to use om.next (or its fork,
[Fulcro](https://github.com/fulcrologic/fulcro)) to make use of the
query language.

> If you're familiar with building apps with om.next/fulcro, please
  note: you often specify a query for each component and compose them
  up. From server-side perspective (which is that of Walkable's), you
  don't know (or care) about those query fragments: you just return
  the right data in the right shape for the big final top-level
  query. The reconciler in the client-side will build up the query and
  break down the result.

> If you know GraphQL, then two main differences between the two query
  languages are:
> - GraphQL is built atop strings, in constrast with Clojure's rich
    data structure used by our query language.
> - om.next query encourages the use of namespaced (aka qualified)
    keywords. In fact, you *must* use namespaced keywords with
    Walkable so it can infer which column is from which table. For
    instance, `:people/id` means the column `id` in the table
    `people`.

## Elements of the query language

### 1. Properties

To query for an entity's properties, use a vector of keywords denoting
which properties you're asking for.

For example, use this query

```clj
[:person/id :person/name :person/age]
```

when you want to receive things like:

```clj
{:person/id   99
 :person/name "Alice"
 :person/age  40}
```

the thing that receives a query and returns the above is called a
query resolver or query parser in some other documentation.

### 2. Joins

Sometimes you want to include another entity that has some kind of
relationship with the current entity.

```clj
{:person/friends [{:person/id   97
                   :person/name "Jon"}
                  {:person/id   98
                   :person/name "Mary"}]
 :person/mate    {:person/id   100
                  :person/name "Bob"}
 :person/pets    [{:pet/id   10
                   :pet/name "Nyan cat"}
                  {:pet/id   20
                   :pet/name "Ceiling cat"}
                  {:pet/id   30
                   :pet/name "Invisible Bike Cat"}]}
```

to achieve that, the query should be:

```clj
[:person/friends :person/mate :person/pets] ;; [^1]
```
which is the same as:

```clj
[{:person/friends [*]}
 {:person/mate    [*]}
 {:person/pets    [*]}]
```

which means you let the query resolver dictate which properties to
return for each join. Or you may explicitly tell your own list:

```clj
[{:person/friends [:person/id :person/name]}
 {:person/mate    [:person/id :person/name]}
 {:person/pets    [:pet/id    :pet/name]}]
```

Wait, isn't this list the vector syntax I've learned in Properties
section? Yup.

You may also notice that the query syntax is the same for to-many
relationships (ie `:person/friends` and `:person/pets`) as well as
to-one one (`:person/mate`). Well, the query resolver, which owns the
data, will decide if it will return an item (as a map) or a vector of
zero or more such item.

### 3. Idents

todo

### 4. Parameters

todo

## Practise

It's recommended to get acquainted with the query language by playing
  with the parser. Give some arbitrary query to
  `fulcro.client.primitives/query->ast` and see the output. Eg:

```clj
(require [fulcro.client.primitives :as prim])

(prim/query->ast [:foo :bar {:yup [:that]}])

;; output:
{:type :root,
 :children [{:type :prop, :dispatch-key :foo, :key :foo}
            {:type :prop, :dispatch-key :bar, :key :bar}
            {:type :join, :dispatch-key :yup, :key :yup, :query [:that],
             :children [{:type :prop, :dispatch-key :that, :key :that}]}]}
```
