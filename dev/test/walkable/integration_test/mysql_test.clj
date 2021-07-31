(ns walkable.integration-test.mysql-test
  {:integration true}
  (:require [walkable.integration-test.helper :refer [run-scenario-tests]]
            [walkable.integration-test.common :refer [common-scenarios]]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [duct.core :as duct]
            [clojure.test :as t :refer [deftest]]))

(duct/load-hierarchy)

(def system
  (-> (duct/read-config (io/resource "config-mysql.edn"))
    (duct/prep-config)
    (ig/init)))

(def db
  (-> system (ig/find-derived-1 :duct.database/sql) val :spec))

(deftest common-scenarios-test
  (run-scenario-tests db :mysql common-scenarios))

(deftest mysql-specific-scenarios-test
  (run-scenario-tests db :mysql {}))
