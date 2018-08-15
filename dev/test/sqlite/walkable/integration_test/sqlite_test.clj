(ns walkable.integration-test.sqlite-test
  (:require [walkable.sql-query-builder :as sqb]
            [walkable.integration-test.helper :refer [run-scenario-tests]]
            [walkable.integration-test.common :refer [common-scenarios]]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [duct.core :as duct]
            [clojure.test :as t :refer [deftest is]]
            [com.wsscode.pathom.core :as p]
            [clojure.java.jdbc :as jdbc]
            [walkable.sql-query-builder.impl.sqlite]))

(duct/load-hierarchy)

(def system
  (-> (duct/read-config (io/resource "config-sqlite.edn"))
    (duct/prep)
    (ig/init)))

(def db
  (-> system (ig/find-derived-1 :duct.database/sql) val :spec))

(deftest common-scenarios-test
  (run-scenario-tests db :sqlite common-scenarios))

(deftest sqlite-specific-scenarios-test
  (run-scenario-tests db :sqlite {}))
