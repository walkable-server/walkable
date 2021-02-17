(ns dev
  (:refer-clojure :exclude [test])
  (:require [clojure.repl :refer :all]
            [fipp.edn :refer [pprint]]
            [clojure.tools.namespace.repl :refer [set-refresh-dirs refresh]]
            [clojure.java.io :as io]
            [duct.core :as duct]
            [eftest.runner :as eftest]
            [integrant.core :as ig]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.profile :as pp]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.connect.planner :as pcp]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [go-loop >! <! put! promise-chan]]
            [integrant.repl :refer [clear halt go init prep reset]]
            [integrant.repl.state :refer [config system]]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.ast :as ast]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.core :as walkable]))

;; <<< Beginning of Duct framework helpers

(duct/load-hierarchy)

(def profiles
  [:duct.profile/dev :duct.profile/local])

(defn config-by-db [db]
  (io/resource (str"config-" (name db) ".edn")))

(defn prepare-system [db]
  (-> (duct/read-config (config-by-db db))
    (duct/prep-config profiles)))

(integrant.repl/set-prep! #(prepare-system :postgres))

(defn test []
  (eftest/run-tests (eftest/find-tests "test")))

(when (io/resource "local.clj")
  (load "local"))

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

;; Simple join examples

;; I named the primary columns "index" and "number" instead of "id" to
;; ensure arbitrary columns will work.

(defn now []
  (.format (java.text.SimpleDateFormat. "HH:mm") (java.util.Date.)))

(require '[walkable.integration-test.helper :refer [walkable-parser]])

(require '[walkable.integration-test.common :as common :refer [farmer-house-registry]])

(def reg (floor-plan/conditionally-update
           farmer-house-registry
           #(= :farmers/farmers (:key %))
           #(merge % {:default-order-by [:farmer/name :desc]
                      :validate-order-by #{:farmer/name :farmer/number}})))

(def w* (walkable-parser :postgres common/person-pet-registry))

(defn w [q]
  (w* {::walkable/db (db) ::walkable/run run-print-query}
    q))

(comment
  (->> (floor-plan/compile-floor-plan* common/person-pet-registry)
    :attributes
    (filter #(= :people/count (:key %)))
    first
    )
    
  (let [f (->> (floor-plan/compile-floor-plan* reg)
            :attributes
            (filter #(= :farmers/farmers (:key %)))
            first
            :compiled-pagination-fallbacks
            :limit-fallback)]
    (f 2))
  
  (w `[{:farmers/farmers
        [:farmer/number :farmer/name
         {:farmer/house [:house/index :house/color]}]}])

  (w `[{(:farmers/farmers {:limit 1})
        [:farmer/number :farmer/name
         {:farmer/house [:house/index :house/color]}]}])

  (w `[(:people/count {:filter [:and {:person/pet [:or [:= :pet/color "white"]
                                                   [:= :pet/color "yellow"]]}
                                [:< :person/number 10]]})])
  )
