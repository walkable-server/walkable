# Walkable

![Walkable logo](doc/walkable.png)

Everything you need from an SQL database should be within walking
distance.

> Data dominates. If you’ve chosen the right data structures and
  organized things well, the algorithms will almost always be
  self-evident. Data structures, not algorithms, are central to
  programming.
> - Rob Pike

> Bad programmers worry about the code. Good programmers worry about
data structures and their relationships.
> - Linus Torvalds

Walkable is a serious way to fetch data from SQL for Clojure:
Datomic® pull syntax, Clojure flavored filtering and more.

[![Build Status](https://travis-ci.org/walkable-server/walkable.svg?branch=master)](https://travis-ci.org/walkable-server/walkable)

Ever imagined sending queries like this to your SQL database?

```clj
[{[:person/by-id 1]
  [:person/id :person/name :person/age
   {:person/friends [:person/name]}
   {:person/pet [:pet/name :pet/favorite-foods]}]}]
```

or a bit more sophisticated:

```clj
[{(:articles/all {:filters [:and [:= false :article/hidden]
                                 {:article/author [:= :author/username "lucy"]
                                  :article/tags   [:in :tag/name "clojure" "clojurescript"]}]})
  [:article/title
   {:article/author [:author/id
                     :author/username]}
   {:article/tags [:tag/name]}]}]
```

Yes, you can. Have your data fetched in your Clojure mission critical
app with confidence. Even more, build the query part of a fulcro
server or REST api in minutes today! Call it from your Clojurescript
app without worrying about SQL injection.

You can learn about the above query language [here](doc/query_language.md)

## Walkable is NOT about om.next

People may have the impression that Walkable (and Pathom) is specific
to om.next. That is NOT the case! Walkable requires a query language
that is expressive and based off data structure. Om.next query
language happens to satisfy that.

Walkable's goal is to become the ultimate SQL library for Clojure.

## Overview

Basically you define your schema like this:

```clj
{:idents           { ;; query with `[:person/by-id 1]` will result in
                    ;; `FROM person WHERE person.id = 1`
                    :person/by-id :person/id
                    ;; just select from `person` table without any constraints
                    :people/all "person"}
 :columns          #{:person/name :person/yob}
 :extra-conditions { ;; enforce some constraints whenever this join is asked for
                    :pet/owner [:and
                                [:= :person/hidden true]
                                ;; yes, you can nest the conditions whatever you like
                                [:or [:= :person/id 5]
                                 ;; a vector implies an `AND` for the conditions inside
                                     [[:in :person/yob
                                       1970
                                       1971
                                       ;; yes, you can!
                                       [:cast "1972" :integer]]
                                      [:like :person/name "john"]]
                                 ;; you can even filter by properties of a join, not just
                                 ;; the item itself
                                 {:person/pet [:or [:= :pet/color "white"]
                                                   [:= :pet/color "green"]]}
                                 ]]}
 :joins            { ;; will produce:
                    ;; "JOIN `person_pet` ON `person`.`id` = `person_pet`.`person_id` JOIN `pet` ON `person_pet`.`pet_id` = `pet`.`id`"
                    :person/pet [:person/id :person-pet/person-id :person-pet/pet-id :pet/id]
                    ;; will produce
                    ;; "JOIN `person` ON `pet`.``owner` = `person`.`id`"
                    :pet/owner [:pet/owner :person/id]}
 :cardinality      {:person/by-id :one
                    :person/pet   :many}}
```

then you can make queries like this:

```clj
'[{(:people/all {:limit    5
                 :offset   10
                 ;; remember the extra-conditions above? you can use the same syntax here:
                 :filters [:or {:person/id [:= 1]}
                           {:person/yob [:in 1999 2000]}]
                 ;; -> you've already limited what the user can access, so let them play freely
                 ;; with whatever left open to them.

                 :order-by [:person/id
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

## Who should use Walkable?

TL;DR: Anyone who have both Clojure and SQL in their toolkit.

To be more specific, anyone at any level of enthusiasm:

 - Any Clojure developers who need to pull data from an SQL database
   (maybe for a web/mobile app, maybe for doing some serious
   calculation, maybe just to migrate away to Datomic :D ) [todo:
   link to comparison with other sql libraries]

 - Clojure enthusiasts who have happen to have ORM-powered apps (ie
   Korma, Django, Rails, most PHP systems, etc.) in production but
   feel overwhelmed by the tedious task of developing new models (or
   dreaded by the error-prone nature of modifying them.)

 - Clojure extremists who use SQL and think Clojure must be used in
   both backend side and frontend side, and the two sides must talk to
   each other using EDN data full of namespaced keywords instead of
   some string-based query language like GraphQL. Think not only
   om.next/fulcro but also reframe/reagent, hoplon, rum/prum, keechma,
   qlkit, quiescent etc.

## Installation

![Latest version](https://clojars.org/walkable/latest-version.svg)

## Usage

Walkable is a plugin for [Pathom](https://github.com/wilkerlucio/pathom/).

> Pathom is a Clojure library designed to provide a collection of
> helper functions to support Clojure(script) graph parsers using
> om.next graph syntax.

(Don't worry if you don't know how Pathom works yet: Understanding
Pathom is not required unless you use advanced features)

Walkable comes with two version: synchronous (for Clojure) and
asynchronous (for both Clojure and Clojurescript).

First of all, you need to build pathom "parser" with walkable's
`sqb/pull-entities` (or `sqb/async-pull-entities`)

```clj
(require '[com.wsscode.pathom.core :as p])
(require '[walkable.sql-query-builder :as sqb])

;; sync version
(def sync-parser
  (p/parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
        ;; walkable's main worker
         [sqb/pull-entities
         ;; pathom's entity reader
          p/map-reader]})]}))

;; async version
(def async-parser
  (p/async-parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
        ;; walkable's main worker
         [sqb/async-pull-entities
         ;; pathom's entity reader
          p/map-reader]})]}))
```

Then you need to define your schema and compile it

```clj
;; both sync and async versions
(def compiled-schema
  (sqb/compile-schema
    {:quote-marks ...
     :columns     ...
     :idents      ...
     :joins       ...
     ...          ...}))
```

Details about the schema is [here](doc/schema.md).

Ready! It's time to run your graph queries:

Sync version:

```clj
(require '[clojure.java.jdbc :as jdbc])

(let [my-query     [{:people/all [:person/name]}]
      my-db        {:dbtype   "mysql"
                    :dbname   "clojure_test"
                    :user     "test_user"
                    :password "test_password"}
      my-run-query jdbc/query]
  (sync-parser {::sqb/sql-db     my-db
                ::sqb/run-query  my-run-query
                ::sqb/sql-schema compiled-schema}
               my-query))
```

where `my-run-query` and `my-db` is any pair of a function plus a
database instance (even a pair of mock ones!) that work together like
this:

```clj
(my-run-query my-db ["select * from fruit where color = ?" "red"])
;; => [{:id 1, :color "red"} {:id 3, :color "red"} ...]

(my-run-query my-db ["select * from fruit"])
;; => [{:id 1, :color "red"} {:id 2, :color "blue"} ...]
```

Async version, Clojure JVM:

```clj
(require '[clojure.java.jdbc :as jdbc])
(require '[clojure.core.async :refer [go promise-chan put! >! <!]])

(let [my-query     [{:people/all [:person/name]}]
      my-db        {:dbtype   "mysql"
                    :dbname   "clojure_test"
                    :user     "test_user"
                    :password "test_password"}
      my-run-query (fn [db q]
                     (let [ch (promise-chan)]
                       (let [result (jdbc/query db q)]
                         (put! ch rersult))
                       ch))]
  (go
    (println
      (<! (async-parser
            {::sqb/sql-db     my-db
             ::sqb/run-query  my-run-query
             ::sqb/sql-schema compiled-schema}
            my-query)))))

```

As you can see, `my-run-query` and `my-db` are similar to those in
sync version, except that `my-run-query` doesn't return the result
directly but in a channel.

For Nodejs, you'll need to convert between Javascript and Clojure data
structure. The file [dev.cljs](dev/src/dev.cljs) has examples using
sqlite3 node module.

## Documentation

- [About the query language](doc/query_language.md)

- [Schema guide](doc/schema.md)

- [Schema, advanced](doc/schema_advanced.md)

- [Filters](doc/filters.md)

- [Developing Walkable](doc/developing.md)

- Please see the file [dev.clj](dev/src/dev.clj) (or its nodejs
version [dev.cljs](dev/src/dev.cljs)) for executable examples. Consult
[config.edn](dev/resources/walkable_demo/config.edn) for SQL
migrations for those examples.

## Special thanks to:

 - Rich Hickey & Cognitect™ team for Clojure and Datomic®

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
  similar queries introduced by the same om.next join query. (aka N+1
  problem)

## Limitation

- Currently Walkable only takes care of reading from the database, NOT
  making mutations to it. I think it varies from applications to
  applications. If you can think of any pattern of doing it, please
  open an issue.

## Support

I'm available for questions regarding walkable on `#walkable`
clojurians slack channel. I'm also on `#fulcro` and
[Clojureverse](https://clojureverse.org/)

## Legal

Copyright © 2018 Hoàng Minh Thắng

Datomic® is a registered trademark of Cognitect, Inc.
