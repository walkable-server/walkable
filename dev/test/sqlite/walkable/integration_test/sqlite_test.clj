(ns walkable.integration-test.sqlite-test
  {:integration true}
  (:require [walkable.integration-test.helper :refer [run-scenario-tests]]
            [walkable.integration-test.common :refer [common-scenarios]]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [duct.core :as duct]
            [clojure.test :refer [deftest]]
            [walkable.sql-query-builder.impl.sqlite]))

(duct/load-hierarchy)

(def system
  (-> (duct/read-config (io/resource "config-sqlite.edn"))
    (duct/prep-config)
    (ig/init)))

(def db
  (-> system (ig/find-derived-1 :duct.database/sql) val :spec))

(deftest common-scenarios-test
  (run-scenario-tests db :sqlite common-scenarios))

(deftest sqlite-specific-scenarios-test
  (run-scenario-tests db :sqlite {}))
