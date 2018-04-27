# Graph Query Syntax

Walkable uses a graph query language to describe what data you want
from your SQL DBMS. The query language was inspired by Datomic's [Pull
API](https://docs.datomic.com/on-prem/pull.html) and
[Graphql](https://graphql.org/) and first introduced in
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
;; query of three prop keys:
[:person/id :person/name :person/age]
```

when you want to receive things like:

```clj
;; returned value
{:person/id   99
 :person/name "Alice"
 :person/age  40}
```

the thing that receives a query and returns the above is called a
query resolver (or simply "query parser" in Om.next, Fulcro and
Pathom's documentation).

### 2. Joins

Sometimes you want to include another entity that has some kind of
relationship with the current entity.

```clj
;; returned value
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
;; query of three join keys:
[:person/friends :person/mate :person/pets]
```
which is the same as:

```clj
;; query
[{:person/friends [*]}
 {:person/mate    [*]}
 {:person/pets    [*]}]
```

which means you let the query resolver dictate which child properties
to return for each join. Or you may explicitly tell your own list:

```clj
;; query
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

Actually the properties and joins above can't stand alone
themselves. They must stem from somewhere: enter idents. Idents are
the root of all queries (or the root of all **evals** :D)

> I lied in the examples in section 1 and 2: such queries are not
  enough for the query resolver to return such results. So what's
  missing? You guess... Idents!

Let's learn about two types of them.

#### 3.1 Keyword idents

Some idents look just like joins:

```clj
;; query
[{:my-profile [:person/name :person/age]}]
```

of course idents can have joins, too:

```clj
;; query
[{:my-profile [:person/name :person/age
               {:person/pets [:pet/name :pet/id]}]}]
```

#### 3.2 Vector idents

Other idents look a bit weird. Instead of being a keyword, they are
made of a vector of a keyword indicating the entity type followed by
exactly one argument specifying how to identify those entities.

First, look at the ident vector:

```clj
;; queries
[:person/by-id 1]
;; or
[:thing/by-uuid "03157713-28f8-4f2b-9aa2-3fc52451369a"]
;; or
[:people-list/by-member-ids #{1 2 3}]
```

Okay, now see them in context:

```clj
;; full queries
[{[:person/by-id 1] [:person/name :person/age]}]
```

`[:person/by-id 1]` is called the ident key. `[:person/name` and
`:person/age` are the child properties.

```clj
[{[:people-list/by-member-ids #{1 2 3}] [:person/name :person/age]}]
```

`[:people-list/by-member-ids #{1 2 3}]` is the ident. Again,
`[:person/name` and `:person/age` are the child properties.

Allow yourself some time to grasp the syntax. Once you're comfortable,
here is the above queries again with a child join added:

```clj
;; queries
[{[:person/by-id 1] [:person/name :person/age
                     {:person/pets [:pet/name :pet/id]}]}]

[{[:people-list/by-member-ids #{1 2 3}] [:person/name :person/age
                                         {:person/pets [:pet/name :pet/id]}]}]
```

You may notice idents also live inside a vector, which means you can
have many of them in your query:

These two queries:

```clj
;; queries
[{[:person/by-id 1] [:person/name :person/age]}]
[{[:person/by-id 2] [:person/name :person/age]}]
```
can be merged into one

```clj
;; query
[{[:person/by-id 1] [:person/name :person/age]}
 {[:person/by-id 2] [:person/name :person/age]}]
```

actually, idents can stem from anywhere, like this:

```clj
;; query
[{[:person/by-id 1] [:person/name :person/age
                     {[:person/by-id 2] [:person/name :person/age]}]}]
```

Here `[:person/by-id 2]` looks just like a child join (such as
`:person/mate`, `:person/pets`), but it has nothing to do with the
entity `[:person/by-id 1]`.

Todo: sample results from each query above.

### 4. Key vs dispatch key

After reading about properties vs joins vs idents, you may think:
"This doesn't make sense! If an ident can sometimes look like a join,
and a join can sometimes look like a property, then what's the point
of defining a syntax at all?

Well, the distinction is less in syntax and more in semantics. For
each key, the author of query resolver should have decided if it's a
property, a join or an ident. Looking at the dispatch key in the key
itself is an efficient way to do it.

What's a dispatch key? Let's learn to recognize them through some
examples:

```
|-------------------+---------------+---------------|
| key               | type          | dispatch-key  |
|-------------------+---------------+---------------|
| :people/all       | keyword ident | :people/all   |
| [:person/by-id 1] | vector ident  | :person/by-id |
| :person/pets      | join          | :human/pets   |
|-------------------+---------------+---------------|

```

### 5. Parameters

Sometimes you want the query resolver to modify a little bit of the
data. Parameters are the piece to communicate just that.

> Parameters must be implemented from the query resolver's side in
  order to have effect. The parameters in the examples below are
  provided to explain the syntax, so you get the idea.

There are two ways to denote parameters, the new and the legacy
syntax. Let's go for the new one first.

#### 5.1 New syntax for parameters

```clj
;; query
'[:person/name :person/height]
;; vs modified query with params in the property `:person/height`
'[:person/name (:person/height {:unit :cm})]
```

Just like a Clojure function's list of arguments, parameters may
contain zero or more items. Personally, I prefer the use of exactly
one hash-map. For instance, with Walkable you can use some pre-defined
parameters:

```clj
;; query with params `{:order-by :person/name}` added to the ident `:people/all`
'[{(:people/all {:order-by :person/name}) [:person/name :person/age]}]

;; query with params `{:offset 20 :limit 10}` added to the ident `:people/all`
[{(:people/all {:offset 20 :limit 10}) [:person/name :person/age]}]
```

Of course, someone else with a different taste may implement their
query resolver to accept keyword parameters instead:

```clj
;; query with params `'(:offset 20 :limit 10)` added to the ident `:people/all`
'[{(:people/all :offset 20 :limit 10) [:person/name :person/age]}]
;; query with params `'(:unit :cm)` in the property `:person/height`
'[:person/name (:person/height :unit :cm)]
```

That's valid syntax, too.

#### 5.2 Legacy syntax for parameters

> This section is required only if you're a user of om.next or fulcro
  older than v2.2.1. Feel free to skip it otherwise.

The syntax for parameters of properties is the same.

For idents and joins, you put the whole query inside a list, followed
by the parameters:

```clj
;; query with params `{:order-by :person/name}` added to the ident `:people/all`
'[({:people/all [:person/name :person/age]}
   {:order-by :person/name})]
;; query with params `{:offset 20 :limit 10}` added to the ident `:people/all`
'[({:people/all [:person/name :person/age]}
   {:offset 20 :limit 10})]
```

These can be quite hard for human to follow if some child join also
have parameters themselves. Fortunately, as you are using om.next /
fulcro to build these queries from smaller ones from components, that
would be no problem at all.

Walkable can work with both new and legacy syntax for params. However
the new one is only supported as of fulcro 2.2.1. If you're using
om.next or older fulcro, you must use the legacy syntax otherwise your
client-side app will crash.

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
