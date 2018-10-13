(ns walkable.integration-test.helper
  (:require [clojure.java.jdbc :as jdbc]
            [com.wsscode.pathom.core :as p]
            [walkable.sql-query-builder :as sqb]
            [clojure.test :refer [testing is]]))

(def walkable-parser
  (p/parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
         [sqb/pull-entities p/env-placeholder-reader p/map-reader]})]}))

(defn db-specific-schema
  [db-type]
  {:quote-marks  (if (= db-type :mysql)
                   sqb/backticks
                   sqb/quotation-marks)
   :sqlite-union (= db-type :sqlite)})

(defn run-scenario-tests
  [db db-type scenarios]
  (into []
    (for [[scenario {:keys [core-schema test-suite]}] scenarios
          {:keys [message env query expected]}        test-suite]
      (testing (str "In scenario " scenario " for " db-type ", testing " message)
        (is (= expected
              (-> {::p/placeholder-prefixes #{"ph"}}
                (merge env {::sqb/sql-db    db
                            ::sqb/run-query jdbc/query
                            ::sqb/sql-schema
                            (sqb/compile-schema
                              (merge core-schema (db-specific-schema db-type)))})
                (walkable-parser query))))))))
