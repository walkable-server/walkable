(ns walkable.integration-test.postgres-test
  {:integration true :postgres true}
  (:require [walkable.integration-test.helper :refer [run-scenario-tests]]
            [walkable.integration-test.common :refer [common-scenarios]]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [duct.core :as duct]
            [clojure.test :as t :refer [deftest use-fixtures]]))

(def ^:dynamic *db* :not-initialized)

(defn setup [f]
  (duct/load-hierarchy)
  (let [system (-> (duct/read-config (io/resource "config-postgres.edn"))
                   (duct/prep-config)
                   (ig/init))]
    (binding [*db* (-> system (ig/find-derived-1 :duct.database/sql) val :spec)]
      (f)
      (ig/halt! system))))

(use-fixtures :once setup)

(deftest common-scenarios-test
  (run-scenario-tests *db* :postgres common-scenarios))

(def planet-inhabitant-registry
  [{:key :land.animal/animals
    :type :root
    :table "land.animal"
    :output [:land.animal/id :land.animal/name]}
   {:key :ocean.animal/animals
    :type :root
    :table "ocean.animal"
    :output [:ocean.animal/id :ocean.animal/name]}
   {:key :land.animal/id
    :type :true-column
    :primary-key true
    :output [:land.animal/name]}
   {:key :ocean.animal/id
    :type :true-column
    :primary-key true
    :output [:ocean.animal/name]}])

(def postgres-scenarios
  {:planet-species
   {:registry planet-inhabitant-registry
    :test-suite
    [{:message "postgres schema should work"
      :query
      `[{:ocean.animal/animals
         [:ocean.animal/id :ocean.animal/name]}
        {[:land.animal/id 1] [:land.animal/id :land.animal/name]}]
      :expected
      {:ocean.animal/animals [#:ocean.animal{:id 10, :name "whale"}
                              #:ocean.animal{:id 20, :name "shark"}]
       [:land.animal/id 1] #:land.animal{:id 1, :name "elephant"}}}]}})

(deftest postgres-specific-scenarios-test
  (run-scenario-tests *db* :postgres postgres-scenarios))
