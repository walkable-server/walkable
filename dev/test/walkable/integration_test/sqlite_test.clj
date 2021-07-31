(ns walkable.integration-test.sqlite-test
  {:integration true :sqlite true}
  (:require [walkable.integration-test.helper :refer [run-scenario-tests]]
            [walkable.integration-test.common :refer [common-scenarios]]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [duct.core :as duct]
            [clojure.test :refer [deftest use-fixtures]]))

(def ^:dynamic *db* :not-initialized)

(defn setup [f]
  (duct/load-hierarchy)
  (let [system (-> (duct/read-config (io/resource "config-sqlite.edn"))
                   (duct/prep-config)
                   (ig/init))]
    (binding [*db* (-> system (ig/find-derived-1 :duct.database/sql) val :spec)]
      (f)
      (ig/halt! system))))

(use-fixtures :once setup)

(deftest common-scenarios-test
  (run-scenario-tests *db* :sqlite common-scenarios))

(deftest sqlite-specific-scenarios-test
  (run-scenario-tests *db* :sqlite {}))
