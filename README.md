# Walkable

Get data from SQL using om.next style query

Ever imagined sending queries like this to your SQL database?

```clj
'[{[:person/by-id 1]
   [:person/id
    :person/name
    :person/yob
    {:person/pet [:pet/id
                  :pet/yob
                  :pet/color
                  {:pet/owner [:person/name]}]}]}]
```

Yes, you can now. Build the query part of a fulcro server or REST api
in minutes today!

## Features

Basically you define your schema like:
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
and you can make queries like this:
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

As you can see the filter syntax is in pure Clojure. It's not just for aesthetic purpose. The generated SQL will always parameterized so it's safe from injection attacks. For instance:
```clj
[:or {:person/name [:like "john"]} {:person/id [:in #{3 4 7}]}]
```
will result in
```clj
["SELECT <...> WHERE person.name LIKE ? OR person.id IN (?, ?, ?)"
"john" 3 4 7]
```

### More use cases

Please see the file [dev.clj](dev/src/dev.clj) for more use
cases. Consult [config.edn](dev/resources/walkable_demo/config.edn)
for SQL migrations for those examples.

## Developing

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
