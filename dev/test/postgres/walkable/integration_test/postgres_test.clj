(ns walkable.integration-test.postgres-test
  {:integration true}
  (:require [walkable.sql-query-builder :as sqb]
            [walkable.integration-test.helper :refer [run-scenario-tests]]
            [walkable.integration-test.common :refer [common-scenarios]]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [duct.core :as duct]
            [clojure.test :as t :refer [deftest is]]
            [com.wsscode.pathom.core :as p]
            [clojure.java.jdbc :as jdbc]
            [walkable.sql-query-builder.impl.postgres]))

(duct/load-hierarchy)

(def system
  (-> (duct/read-config (io/resource "config-postgres.edn"))
    (duct/prep)
    (ig/init)))

(def db
  (-> system (ig/find-derived-1 :duct.database/sql) val :spec))

(deftest common-scenarios-test
  (run-scenario-tests db :postgres common-scenarios))

(def planet-inhabitant-schema
  {:columns          #{:land.animal/id :land.animal/name
                       :ocean.animal/id :ocean.animal/name}
   :idents           {:land.animal/all    "land.animal"
                      :land.animal/by-id  :land.animal/id
                      :ocean.animal/all   "ocean.animal"
                      :ocean.animal/by-id :ocean.animal/id}
   :cardinality      {:land.animal/by-id  :one
                      :ocean.animal/by-id :one}})

(def postgres-scenarios
  {:planet-species
   {:core-schema planet-inhabitant-schema
    :test-suite
    [{:message "postgres schema should work"
      :query
      `[{[:land.animal/by-id 1]
         [:land.animal/id :land.animal/name]}
        {:ocean.animal/all
         [:ocean.animal/id :ocean.animal/name]}]
      :expected
      {[:land.animal/by-id 1] #:land.animal {:id 1, :name "elephant"},
       :ocean.animal/all      [#:ocean.animal{:id 10, :name "whale"}
                               #:ocean.animal{:id 20, :name "shark"}]}}]}})

(deftest postgres-specific-scenarios-test
  (run-scenario-tests db :postgres postgres-scenarios))
