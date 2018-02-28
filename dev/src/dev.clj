(ns dev
  (:refer-clojure :exclude [test])
  (:require [clojure.repl :refer :all]
            [fipp.edn :refer [pprint]]
            [clojure.tools.namespace.repl :refer [refresh]]
            [clojure.java.io :as io]
            [duct.core :as duct]
            [duct.core.repl :as duct-repl]
            [duct.repl.figwheel :refer [cljs-repl]]
            [eftest.runner :as eftest]
            [integrant.core :as ig]
            [com.wsscode.pathom.core :as p]
            [walkable-demo.handler.example :refer [pathom-parser]]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [integrant.repl :refer [clear halt go init prep reset]]
            [integrant.repl.state :refer [config system]]
            [walkable-demo.handler.example :as example]
            [walkable.sql-query-builder :as sqb]))

(defn db []
  (-> system (ig/find-derived-1 :duct.database/sql) val :spec))

(defn q [sql]
  (jdbc/query (db) sql))

(defn e [sql]
  (jdbc/execute! (db) sql))

(derive ::devcards :duct/module)

(defmethod ig/init-key ::devcards [_ {build-id :build-id :or {build-id 0}}]
  {:req #{:duct.server/figwheel}
   :fn  #(assoc-in % [:duct.server/figwheel :builds build-id :build-options :devcards] true)})

(duct/load-hierarchy)

(defn read-config []
  (duct/read-config (io/resource "dev.edn")))

(defn test []
  (eftest/run-tests (eftest/find-tests "test")))

(clojure.tools.namespace.repl/set-refresh-dirs "dev/src" "dev/test" "src" "test")

(when (io/resource "local.clj")
  (load "local"))

(integrant.repl/set-prep! (comp duct/prep read-config))

(comment
  (e "CREATE TABLE `foo` (`id` INTEGER)")
  (e "INSERT INTO `foo` (`id`) VALUES (1)")
  (q "SELECT * from foo")
  (e "DROP TABLE `foo`")
  )

(defn run-print-query [& xs]
  (let [[q & args] (rest xs)]
    (println q)
    (println args))
  (apply jdbc/query xs))

;; simple join examples
#_
(let [eg-1
      '[{[:farmer/by-id 1] [:farmer/number :farmer/name
                            {:farmer/cow [:cow/index :cow/color]}]}]

      parser
      example/pathom-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query

           ::sqb/sql-schema
           (sqb/compile-schema
             {:columns          [:cow/index
                                 :cow/color
                                 :farmer/cow-index
                                 :farmer/number
                                 :farmer/name]
              :idents           {:farmer/by-id [:= :farmer/number]
                                 :farmers/all  "farmer"}
              :extra-conditions {}
              :joins            {:farmer/cow [:farmer/cow-index :cow/index]}
              :reversed-joins   {:cow/owner :farmer/cow}
              :join-cardinality {:farmer/by-id :one
                                 :cow/owner    :one
                                 :farmer/cow   :one}})}
    eg-1))

#_
(let [eg-1
      '[{[:kid/by-id 1] [:kid/number :kid/name
                         {:kid/toy [:toy/index :toy/color]}]}]

      parser
      example/pathom-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query

           ::sqb/sql-schema
           (sqb/compile-schema
             {:columns          [:kid/number :kid/name :toy/index :toy/color :toy/owner-number]
              :idents           {:kid/by-id [:= :kid/number]
                                 :kids/all  "kid"}
              :extra-conditions {}
              :joins            {:toy/owner [:toy/owner-number :kid/number]}
              :reversed-joins   {:kid/toy :toy/owner}
              :join-cardinality {:kid/by-id :one
                                 :kid/toy   :one
                                 :toy/owner :one}})}
    eg-1))

;; filters and conditions example
#_
(let [eg-1
      '[{(:people/all {::sqb/filters  {:person/number [:< 10]}
                       ;; ::sqb/limit    1
                       ;; ::sqb/offset   1
                       ;; ::sqb/order-by [:person/name :desc]
                       })
         [:person/number :person/name
          {:person/pet [:pet/index
                        :pet/age
                        :pet/color]}]}]

      eg-2
      '[{[:person/by-id 1]
         [:person/number
          :person/name
          :person/age
          {:person/pet [:pet/index
                        :pet/age
                        :pet/color
                        {:pet/owner [:person/name]}]}]}]
      parser
      example/pathom-parser]
  (parser {;; extra env data, eg current user id provided by Ring session
           :current-user   1

           ::sqb/sql-db    (db)
           ::sqb/run-query run-print-query
           ::sqb/sql-schema
           (sqb/compile-schema
             ;; which columns are available in SQL table?
             {:columns          [:person/number
                                 :person/name
                                 :person/yob
                                 :person/hidden
                                 :person-pet/person-number
                                 :person-pet/pet-index
                                 :pet/index
                                 :pet/name
                                 :pet/yob
                                 :pet/color]
              ;; extra columns required when an attribute is being asked for
              ;; can be input to derive attributes, or parameters to other attribute resolvers that will run SQL queries themselves
              :required-columns {:pet/age    #{:pet/yob}
                                 :person/age #{:person/yob}}
              :idents           {:person/by-id [:= :person/number]
                                 :people/all   "person"}
              :extra-conditions {[:pet/owner :person/by-id]
                                 [:or {:person/hidden [:= true]}
                                  {:person/hidden [:= false]}]}
              :joins            {:person/pet [:person/number :person-pet/person-number
                                              :person-pet/pet-index :pet/index]}
              :reversed-joins   {:pet/owner :person/pet}
              :join-cardinality {:person/by-id :one
                                 :person/pet   :many}})}
    eg-1))

;; self-join example
#_
(let [eg-1
      '[{:world/all
         [:human/number :human/name :human/two
          {:human/follow-stats [:follow/count]}
          {:human/follow [:human/number
                          :human/name
                          :human/yob]}]}]
      parser
      example/pathom-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query
           ::sqb/sql-schema
           (sqb/compile-schema
             {:columns          [:human/number :human/name :human/yob
                                 ;; :human-1/number :human-2/number
                                 ]
              :required-columns {}
              :idents           {:human/by-id [:= :human/number]
                                 :world/all   "human"}
              :extra-conditions {}
              :joins            {;; technically :human/follow and :human/follow-stats are the same join
                                 ;; but they have different cardinality
                                 [:human/follow :human/follow-stats]
                                 [:human/number :follow/human-1 :follow/human-2 :human/number]}
              :reversed-joins   {}
              :pseudo-columns   {;; using sub query as a column
                                 :human/two "(SELECT 2)"
                                 ;; using aggregate as a column
                                 :follow/count "COUNT(`follow`.`human_2`)"}
              :join-cardinality {:human/by-id  :one
                                 :human/follow-stats :one
                                 :human/follow :many}})}
    eg-1))
