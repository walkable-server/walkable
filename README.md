![Walkable logo](doc/walkable.png)

# Walkable

Everything you need from an SQL database should be within walking
distance.

Walkable is a serious way to fetch data from SQL for Clojure:
Datomic/om.next pull syntax, Clojure flavored filtering and more.

[![Build Status](https://travis-ci.org/walkable-server/walkable.svg?branch=master)](https://travis-ci.org/walkable-server/walkable)

Ever imagined sending queries like this to your SQL database?

```clj
[{[:person/by-id 1]
  [:person/id :person/name :person/age
   {:person/friends [:person/name]}
   {:person/pet [:pet/name :pet/favorite-foods]}]}]
```

Yes, you can. Have your data fetched in your Clojure mission critical
app with confidence. Even more, build the query part of a fulcro
server or REST api in minutes today! Call it from your Clojurescript
app without worrying about SQL injection.

You can learn about the above query language [here](doc/query_language.md)

## Overview

Basically you define your schema like this:

```clj
{:idents           {;; query with `[:person/by-id 1]` will result in
                    ;; `FROM person WHERE person.id = 1`
                    :person/by-id [:= :person/id]
                    ;; just select from `person` table without any constraints
                    :people/all "person"}
 :extra-conditions {;; enforce some constraints whenever this join is asked for
                    :pet/owner [:and
                                {:person/hidden [:= true]}
                                ;; yes, you can nest the conditions whatever you like
                                [:or
                                 {:person/id [:not= 5]}
                                  ;; a hashmap implies an `AND` for the k/v pairs inside
                                 {:person/yob [:in 1970 1971 1972]
                                  :person/name [:like "john"]}

                                 ;; even this style of condition
                                 {:person/name [:or
                                                [:like "john"]
                                                [:like "mary"]]}]]}
 :joins            {;; will produce:
                    ;; "JOIN `person_pet` ON `person`.`id` = `person_pet`.`person_id` JOIN `pet` ON `person_pet`.`pet_id` = `pet`.`id`"
                    :person/pet [:person/id :person-pet/person-id :person-pet/pet-id :pet/id]
                    ;; will produce
                    ;; "JOIN `person` ON `pet`.``owner` = `person`.`id`"
                    :pet/owner [:pet/owner :person/id]}
 :join-cardinality {:person/by-id :one
                    :person/pet   :many}}
```

then you can make queries like this:

```clj
'[{(:people/all {::sqb/limit    5
                 ::sqb/offset   10
                  ;; remember the extra-conditions above? you can use the same syntax here:
                 ::sqb/filters [:or {:person/id [:= 1]}
                                {:person/yob [:in 1999 2000]}]
                 ;; -> you've already limited what the user can access, so let them play freely
                 ;; with whatever left open to them.

                 ::sqb/order-by [:person/id
                                 :person/name :desc
                                 ;; Note: sqlite doesn't support `:nils-first`, `:nils-last`
                                 :person/yob :desc :nils-last]})
   [:person/id :person/name]}]
```

As you can see the filter syntax is in pure Clojure. It's not just for
aesthetic purpose. The generated SQL will always parameterized so it's
safe from injection attacks. For instance:

```clj
[:or {:person/name [:like "john"]} {:person/id [:in #{3 4 7}]}]
```

will result in

```clj
["SELECT <...> WHERE person.name LIKE ? OR person.id IN (?, ?, ?)"
"john" 3 4 7]
```

## Installation

![Latest version](https://clojars.org/walkable/latest-version.svg)

## Usage

Walkable is a plugin for [Pathom](https://github.com/wilkerlucio/pathom/).

> Pathom is a Clojure library designed to provide a collection of
> helper functions to support Clojure(script) graph parsers using
> om.next graph syntax.

First of all, you need to build pathom parser with walkable's `sqb/pull-entities`

```clj
(require '[com.wsscode.pathom.core :as p])
(require '[walkable.sql-query-builder :as sqb])

(def pathom-parser
  (p/parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
        ;; walkable's main worker
         [sqb/pull-entities
         ;; pathom's entity reader
          p/map-reader]})]}))
```

Then you need to define your schema and compile it

```clj
(def compiled-schema
  (sqb/compile-schema
    {:quote-marks ...
     :columns     ...
     :idents      ...
     :joins       ...
     ...          ...}))
```

Ready! It's time to run your graph queries

```clj
(require '[clojure.java.jdbc :as jdbc])

(let [my-query     [{:people/all [:person/name]}]
      my-db        {:dbtype   "mysql"
                    :dbname   "clojure_test"
                    :user     "test_user"
                    :password "test_password"}
      my-run-query jdbc/query]
  (pathom-parser {::sqb/sql-db    my-db
                  ::sqb/run-query my-run-query
                  ::sqb/schema    compiled-schema}))
```

where `my-run-query` and `my-db` is any pair of a function plus a
database spec (even a pair of mock ones!) that work together like
this:

```clj
(my-run-query my-db ["select * from fruit where color = ?" "red"])
;; => [{:id 1, :color "red"} {:id 3, :color "red"} ...]

(my-run-query my-db "select * from fruit")
;; => [{:id 1, :color "red"} {:id 2, :color "blue"} ...]
```

### More use cases

Please see the file [dev.clj](dev/src/dev.clj) for more use
cases. Consult [config.edn](dev/resources/walkable_demo/config.edn)
for SQL migrations for those examples.

### Note on examples' query params syntax

Walkable works with both `[({:k subquery} params)]` and `[{(:k
params) subquery}]` syntax but in the examples I always use the later
due to my personal preference. However it's only supported as of fulcro
2.2.1. If you're using om.next or older fulcro, you must use the
former syntax otherwise your client-side app will crash.

## Special thanks to:

 - Rich Hickey & Cognitect team for Clojure and Datomic

 - David Nolen for bringing many fresh ideas to the community including om.next

 - James Reeves for Duct framework. The best development experience I've ever had

 - Tony Kay for his heroic work on fulcro which showed me how great things can be done

 - Wilker Lucio for pathom and being helpful with my silly questions

 - Bozhidar Batsov and CIDER team!!!

## Performance

Walkable comes with some optimizations:

- A compile phase (`sqb/compile-schema`) that pre-computes many parts
  of final SQL query strings.

- Reduce roundtrips between Clojure and SQL server by combining
  similar queries introduced by the same om.next join query.

## Limitation

- Currently Walkable only takes care of reading from the database, NOT
  making mutations to it. I think it varies from applications to
  applications. If you can think of any pattern of doing it, please
  open an issue.

- Walkable does not support async sql query runners, so it's a no go for
nodejs.

## Support

I'm available for questions regarding walkable on `#walkable`
clojurians slack channel. I'm also on `#fulcro` and
[Clojureverse](https://clojureverse.org/)

## Developing

See [developing.md](doc/developing.md)

## Legal

Copyright © 2018 Hoàng Minh Thắng
