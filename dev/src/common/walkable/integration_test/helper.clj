(ns walkable.integration-test.helper
  (:require [clojure.java.jdbc :as jdbc]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [walkable.core :as walkable]
            [walkable.sql-query-builder.emitter :as emitter]
            [clojure.test :refer [testing is]]))

(def db-specific-emitter
  {:mysql    emitter/mysql-emitter
   :sqlite   emitter/sqlite-emitter
   :postgres emitter/postgres-emitter})

(defn connect-config
  [{:keys [db-type core-config core-floor-plan cte?]}]
  (let [resolver-sym `test-resolver]
    (merge core-config
      {:resolver-sym resolver-sym

       :floor-plan
       (merge core-floor-plan
         {:emitter (db-specific-emitter db-type)
          :use-cte (when cte? {:default true})})})))

(defn walkable-parser
  [{:keys [db] :as config}]
  (p/parser
    {::p/env     {::p/reader               [p/map-reader
                                            pc/reader3
                                            pc/open-ident-reader
                                            p/env-placeholder-reader]}
     ::p/plugins [(pc/connect-plugin {::pc/register []})
                  (walkable/connect-plugin (assoc (connect-config config)
                                             :db db
                                             :query jdbc/query))
                  p/elide-special-outputs-plugin
                  p/error-handler-plugin
                  p/trace-plugin]}))

(defn run-scenario-tests
  [db db-type scenarios]
  (into []
    (for [[scenario {:keys [core-floor-plan core-config test-suite]}] scenarios
          {:keys [message env query expected]}            test-suite]
      (testing (str "In scenario " scenario " for " db-type ", testing " message)
        (is (= expected
              (let [parser (walkable-parser {:db db
                                             :core-config core-config
                                             :core-floor-plan core-floor-plan
                                             :db-type db-type})]
                (parser env query)))
          "without CTEs in joins")

        (is (= expected
              (let [parser (walkable-parser {:db db
                                             :core-config core-config
                                             :core-floor-plan core-floor-plan
                                             :db-type db-type
                                             :cte? true})]
                (parser env query)))
          "with CTEs in joins")))))
