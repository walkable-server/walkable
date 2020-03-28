(ns walkable.integration-test.postgres-test
  {:integration true}
  (:require [walkable.integration-test.helper :refer [run-scenario-tests]]
            [walkable.integration-test.common :refer [common-scenarios]]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [duct.core :as duct]
            [com.wsscode.pathom.connect :as pc]
            [clojure.test :as t :refer [deftest]]
            [walkable.sql-query-builder.impl.postgres]))

(duct/load-hierarchy)

(def system
  (-> (duct/read-config (io/resource "config-postgres.edn"))
    (duct/prep-config)
    (ig/init)))

(def db
  (-> system (ig/find-derived-1 :duct.database/sql) val :spec))

(deftest common-scenarios-test
  (run-scenario-tests db :postgres common-scenarios))

(def planet-inhabitant-floor-plan
  {:true-columns #{:land.animal/id :land.animal/name
                   :ocean.animal/id :ocean.animal/name}
   :roots        {:land.animal/animals  "land.animal"
                  :ocean.animal/animals "ocean.animal"}
   :cardinality  {:land.animal/id  :one
                  :ocean.animal/id :one}})

(def planet-inhabitant-config
  {:inputs-outputs
   [{::pc/output [{:land.animal/animals [:land.animal/id :land.animal/name]}]}

    {::pc/output [{:ocean.animal/animals [:ocean.animal/id :ocean.animal/name]}]}

    {::pc/input  #{:land.animal/id}
     ::pc/output [:land.animal/name]}

    {::pc/input  #{:ocean.animal/id}
     ::pc/output [:ocean.animal/name]}]})

(def postgres-scenarios
  {:planet-species
   {:core-floor-plan planet-inhabitant-floor-plan
    :core-config     planet-inhabitant-config
    :test-suite
    [{:message "postgres schema should work"
      :query
      `[{:ocean.animal/animals
         [:ocean.animal/id :ocean.animal/name]}
        {[:land.animal/id 1] [:land.animal/id :land.animal/name]}]
      :expected
      {:ocean.animal/animals [#:ocean.animal{:id 10, :name "whale"}
                              #:ocean.animal{:id 20, :name "shark"}]
       [:land.animal/id 1]   #:land.animal {:id 1, :name "elephant"}}}]}})

(deftest postgres-specific-scenarios-test
  (run-scenario-tests db :postgres postgres-scenarios))
