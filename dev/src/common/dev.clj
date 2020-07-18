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
  (case db
    :postgres (require '[walkable.sql-query-builder.impl.postgres])
    :sqlite   (require '[walkable.sql-query-builder.impl.sqlite])
    true)
  (integrant.repl/set-prep! #(prepare-system db)))

;; automatically set target db
(cond
  (config-by-db :postgres) (set-target-db! :postgres)
  (config-by-db :sqlite)   (set-target-db! :sqlite)
  (config-by-db :mysql)    (set-target-db! :mysql))

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

(def emitter emitter/default-emitter)

;; Simple join examples

;; I named the primary columns "index" and "number" instead of "id" to
;; ensure arbitrary columns will work.

(defn now []
  (.format (java.text.SimpleDateFormat. "HH:mm") (java.util.Date.)))

(def inputs-outputs
  (let [farmer-out [:farmer/number
                    :farmer/name
                    :farmer/house-index
                    :farmer/house-count
                    :farmer/house]
        house-out  [:house/index
                    :house/color
                    :house/owner]]
    [{::pc/output [{:farmers/farmers farmer-out}]}

     {::pc/input  #{:farmer/number}
      ::pc/output farmer-out}

     {::pc/output [{:house/owner farmer-out}]}

     {::pc/output [{:houses/houses house-out}]}

     {::pc/output [{:farmer/house house-out}]}

     {::pc/input  #{:house/index}
      ::pc/output house-out}]))

(require '[plumbing.core :refer [fnk]])

(def core-config
  (let [resolver-sym `my-resolver]
    {:resolver-sym   resolver-sym
     :inputs-outputs inputs-outputs

     :floor-plan
     {:emitter          emitter
      ;; columns already declared in :joins are not needed
      ;; here
      :true-columns     [:house/color
                         :farmer/number
                         :farmer/name]
      :idents           #{:house/index :farmer/number}
      :roots            {:farmers/farmers "farmer"
                         :houses/houses   "house"}
      :aggregators      {:farmer/house-count [:count-*]}
      :use-cte          {:default false}
      :extra-conditions {}
      :joins            {[:farmer/house :farmer/house-count]
                         [:farmer/house-index :house/index]}
      :reversed-joins   {:house/owner :farmer/house}
      :cardinality      {;; :farmer/number :one
                         ;; :house/index :one
                         :house/owner  :one
                         :farmer/house :one}}}))

#_
(let [eg-1
      '[{(:farmers/farmers #_{:filters {:farmer/house [{:house/owner [:= :farmer/name "mary"]}
                                                   [:= :house/color "brown"]]}})
         [:farmer/number
          :farmer/name
          {:farmer/house [:house/index
                          :house/color
                          #_{:house/owner
                           [:farmer/name
                            :farmer/number
                            {:farmer/house [:house/index
                                            :house/color
                                            {:house/owner [:farmer/number
                                                           :farmer/name]}]}]}]}]}

        {#_:houses/houses
         [:house/index "10"]
         [:house/index
          :house/color

          {:>/else [{:house/owner [:farmer/number
                                   :farmer/name
                                   #_{:farmer/house [:house/index
                                                   :house/color
                                                   {:house/owner [:farmer/name]}]}]}]}
          #_
          {:house/owner [:farmer/name
                         {:farmer/house [;; :house/index
                                         :house/color
                                         {:house/owner [:farmer/name]}]}]}]}]
      eg-2
      '[{:houses/houses
         #_[:house/index "20"]
         [:house/index
          :house/color
          #_
          {:>/else
           [{:house/owner [:farmer/name
                           :farmer/number
                           {:farmer/house [:house/index
                                           :house/color
                                           {:house/owner [:farmer/number
                                                          :farmer/name]}]}]}]}

          {:house/owner [:farmer/name
                         :farmer/number ;; <----------- not automatically injected yet. Source column is there but not look up column
                         :farmer/house-index
                         {:farmer/house [:house/index
                                         :house/color
                                         {:house/owner [:farmer/name
                                                        ;:farmer/house-index
                                                        :farmer/number
                                                        ]}]}]}]}]

      config
      (merge {:db    (db)
              :query jdbc/query #_run-print-query}
        core-config)

      plg
      (walkable/connect-plugin config)

      the-parser
      (p/parser
        {::p/env     {::p/reader               [p/map-reader
                                                pc/reader3
                                                pc/open-ident-reader
                                                p/env-placeholder-reader]
                      ::p/placeholder-prefixes #{">"}}
         ::p/mutate  pc/mutate
         ::p/plugins [(pc/connect-plugin {::pc/register []})
                      plg
                      p/elide-special-outputs-plugin
                      p/error-handler-plugin
                      p/trace-plugin]})]
  (println "running at " (now) "\n\n")
  (the-parser {} eg-1))
