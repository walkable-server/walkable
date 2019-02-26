(ns walkable.integration-test.helper
  (:require [clojure.java.jdbc :as jdbc]
            [com.wsscode.pathom.core :as p]
            [walkable.sql-query-builder :as sqb]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [clojure.test :refer [testing is]]))

(def walkable-parser
  (p/parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
         [sqb/pull-entities p/env-placeholder-reader p/map-reader]})]}))

(def db-specific-emitter
  {:mysql    emitter/mysql-emitter
   :sqlite   emitter/sqlite-emitter
   :postgres emitter/postgres-emitter})

(defn run-scenario-tests
  [db db-type scenarios]
  (into []
    (for [[scenario {:keys [core-floor-plan test-suite]}] scenarios
          {:keys [message env query expected]}            test-suite]
      (testing (str "In scenario " scenario " for " db-type ", testing " message)
        (is (= expected
              (-> {::p/placeholder-prefixes #{"ph"}}
                (merge env {::sqb/sql-db    db
                            ::sqb/run-query jdbc/query
                            ::sqb/floor-plan
                            (floor-plan/compile-floor-plan
                              (merge core-floor-plan
                                {:emitter (db-specific-emitter db-type)}))})
                (walkable-parser query)))
          "without CTEs in joins")
        (is (= expected
              (-> {::p/placeholder-prefixes #{"ph"}}
                (merge env {::sqb/sql-db    db
                            ::sqb/run-query jdbc/query
                            ::sqb/floor-plan
                            (floor-plan/compile-floor-plan
                              (merge core-floor-plan
                                {:emitter (db-specific-emitter db-type)
                                 :use-cte {:default true}}))})
                (walkable-parser query)))
          "with CTEs in joins")))))
