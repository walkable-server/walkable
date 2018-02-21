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
  {:req #{:duct.module/cljs :duct.server/figwheel}
   :fn  #(assoc-in % [:duct.server/figwheel :builds build-id :build-options :devcards] true)})

(duct/load-hierarchy)

(defn read-config []
  (duct/read-config (io/resource "dev.edn")))

(defn test []
  (eftest/run-tests (eftest/find-tests "test")))

(clojure.tools.namespace.repl/set-refresh-dirs "dev/src" "src" "test")

(when (io/resource "local.clj")
  (load "local"))

(integrant.repl/set-prep! (comp duct/prep read-config))

(comment
  (e "CREATE TABLE `foo` (`id` INTEGER)")
  (e "INSERT INTO `foo` (`id`) VALUES (1)")
  (q "SELECT * from foo")
  (e "DROP TABLE `foo`")
  )
#_
(let [eg-1
      '[{(:people/all {::sqb/limit 1
                       ::sqb/offset 1
                       ::sqb/order-by [[:person/name :desc]]})
         [:person/number :person/name]}]

      eg-2
      '[{([:person/by-id 1] {::sqb/filters {:person/number [:= 1]}})
         [:person/number
          :person/name
          :person/age
          {:person/pet [:pet/index
                        :pet/age
                        :pet/color
                        {:pet/owner [:person/name]}]}]}]
      parser
      example/pathom-parser]
  (parser {:current-user 1
           ::sqb/sql-db    (db)
           ::sqb/run-query
           (fn [& xs]
             (let [[q & args] (rest xs)]
               (println q)
               (println args))
             (apply jdbc/query xs))
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
                                 :people/all "person"}
              :extra-conditions {[:pet/owner :person/by-id]
                                 [:or {:person/hidden [:= true]}
                                  {:person/hidden [:= false]}]}
              :joins            {:person/pet [:person/number :person-pet/person-number
                                              :person-pet/pet-index :pet/index]}
              :reversed-joins   {:pet/owner :person/pet}
              :join-cardinality {:person/by-id :one
                                 :person/pet   :many}})}
    eg-1))
