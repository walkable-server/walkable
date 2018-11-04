(ns dev
  (:refer-clojure :exclude [test])
  (:require [clojure.repl :refer :all]
            [fipp.edn :refer [pprint]]
            [clojure.tools.namespace.repl :refer [set-refresh-dirs refresh]]
            [clojure.java.io :as io]
            [duct.core :as duct]
            [duct.core.repl :as duct-repl]
            [eftest.runner :as eftest]
            [integrant.core :as ig]
            [duct.database.sql :as sql]
            [hikari-cp.core :as hikari-cp]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.profile :as pp]
            [com.wsscode.pathom.connect :as pc]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [go-loop >! <! put! promise-chan]]
            ;; or walkable.sql-query-builder.impl.postgres
            [walkable.sql-query-builder.impl.sqlite]
            [integrant.repl :refer [clear halt go init prep reset]]
            [integrant.repl.state :refer [config system]]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.sql-query-builder :as sqb]))

;; <<< Beginning of Duct framework helpers

(duct/load-hierarchy)

(defn prepare-system [db]
  (-> (duct/read-config (io/resource (str"config-" (name db) ".edn")))
    (duct/prep)))

(defn test []
  (eftest/run-tests (eftest/find-tests "test")))

(when (io/resource "local.clj")
  (load "local"))

(defn set-target-db! [db]
  (assert (#{:postgres :mysql :sqlite} db))
  (set-refresh-dirs
    "dev/src/common"
    (str "dev/src/" ({:postgres "postgres" :mysql "mysql"} db "sqlite"))
    "src" "test")
  (integrant.repl/set-prep! #(prepare-system db)))

(comment
  (set-target-db! :postgres)
)

(defn db []
  (-> system (ig/find-derived-1 :duct.database/sql) val :spec))

(defn q [sql]
  (jdbc/query (db) sql))

(defn e [sql]
  (jdbc/execute! (db) sql))

(comment
;; make changes to database right from your editor
  (e "CREATE TABLE `foo` (`id` INTEGER)")
  (e "INSERT INTO `foo` (`id`) VALUES (1)")
  (q "SELECT * from foo")
  (e "DROP TABLE `foo`")
  )

;; End of Duct framework helpers >>>

(defn run-print-query
  "jdbc/query wrapped by println"
  [& xs]
  (let [[q & args] (rest xs)]
    (println q)
    (println args))
  (apply jdbc/query xs))

(defn async-run-print-query
  [db q]
  (let [c (promise-chan)]
    (let [r (run-print-query db q)]
      ;; (println "back from sql: " r)
      (put! c r))
    c))

(def sync-parser
  (p/parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
         [sqb/pull-entities p/map-reader
          pc/all-readers]})]}))

(def async-parser
  (p/async-parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
         [sqb/async-pull-entities p/map-reader]})]}))

(def emitter emitter/default-emitter)

;; Simple join examples

;; I named the primary columns "index" and "number" instead of "id" to
;; ensure arbitrary columns will work.

;; Example: join column living in source table
#_
(let [eg-1
      '[{(:farmers/all {:filters {:farmer/cow [{:cow/owner [:= :farmer/name "mary"]}
                                               [:= :cow/color "brown"]]}})
         [:farmer/number :farmer/name
          {:farmer/cow [:cow/index :cow/color]}]}]

      parser
      sync-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query

           ::sqb/floor-plan
           (floor-plan/compile-floor-plan
             {:emitter          emitter
              ;; columns already declared in :joins are not needed
              ;; here
              :columns          [:cow/color
                                 :farmer/number
                                 :farmer/name]
              :idents           {:farmer/by-id :farmer/number
                                 :farmers/all  "farmer"}
              :extra-conditions {}
              :joins            {:farmer/cow [:farmer/cow-index :cow/index]}
              :reversed-joins   {:cow/owner :farmer/cow}
              :cardinality      {:farmer/by-id :one
                                 :cow/owner    :one
                                 :farmer/cow   :one}})}
    eg-1))

;; the same above, but using async version
#_
(let [eg-1
      '[{[:farmer/by-id 1] [:farmer/number :farmer/name
                            {:farmer/cow [:cow/index :cow/color]}]}]

      parser
      async-parser]
  (async/go
    (println "final result"
      (<! (parser {::sqb/sql-db    (db)
                   ::sqb/run-query async-run-print-query

                   ::sqb/floor-plan
                   (floor-plan/compile-floor-plan
                     {:emitter          emitter
                      ;; columns already declared in :joins are not needed
                      ;; here
                      :columns          [:cow/color
                                         :farmer/number
                                         :farmer/name]
                      :idents           {:farmer/by-id :farmer/number
                                         :farmers/all  "farmer"}
                      :extra-conditions {}
                      :joins            {:farmer/cow [:farmer/cow-index :cow/index]}
                      :reversed-joins   {:cow/owner :farmer/cow}
                      :cardinality      {:farmer/by-id :one
                                         :cow/owner    :one
                                         :farmer/cow   :one}})}
            eg-1)))))
