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
            [integrant.repl.state :refer [config system]]))

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
