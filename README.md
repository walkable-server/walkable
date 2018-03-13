# Walkable

Everything you need from an SQL database should be within walking
distance.[1]

Walkable is a serious way to fetch data from SQL using Clojure:
Datomic/om.next *pull* syntax, Clojure flavored filtering and more.

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

[1]: By within walking distance I mean composing your queries without
missing the power of paredit/parinfer.

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

## Developing

Walkable comes with a [Duct](https://github.com/duct-framework/duct)
setup as its development environment which can be found in `dev`
directory.

> Duct is a highly modular framework for building server-side
> applications in Clojure using data-driven architecture.

Here is a quick guide for how to use the development environment.

> A more detailed guide for Duct can be found at:
>
> https://github.com/duct-framework/docs/blob/master/GUIDE.rst

### Setup

When you first clone this repository, run:

```sh
lein duct setup
```

This will create files for local configuration, and prep your system
for the project.

### Environment

To begin developing, start with a REPL.

```sh
lein repl
```

Then load the development environment.

```clojure
user=> (dev)
:loaded
```

Run `go` to prep and initiate the system.

```clojure
dev=> (go)
:duct.server.http.jetty/starting-server {:port 3000}
:initiated
```

By default this creates a web server at <http://localhost:3000>.

When you make changes to your source files, use `reset` to reload any
modified files and reset the server. Changes to CSS or ClojureScript
files will be hot-loaded into the browser.

```clojure
dev=> (reset)
:reloading (...)
:resumed
```

If you want to access a ClojureScript REPL, make sure that the site is loaded
in a browser and run:

```clojure
dev=> (cljs-repl)
Waiting for browser connection... Connected.
To quit, type: :cljs/quit
nil
cljs.user=>
```

### Testing

Testing is fastest through the REPL, as you avoid environment startup
time.

```clojure
dev=> (test)
...
```

But you can also run tests through Leiningen.

```sh
lein test
```

## Legal

Copyright © 2018 Hoàng Minh Thắng
