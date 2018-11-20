(ns walkable.integration-test.sqlite-test
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [walkable.integration-test.common :refer [common-scenarios]]
            [cljs.core.async :as async :refer [put! >! <! promise-chan]]
            [clojure.test :as t :refer [deftest testing is async]]
            [com.wsscode.pathom.core :as p]
            [walkable.sql-query-builder :as sqb]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            ["sqlite3" :as sqlite3]
            [walkable.sql-query-builder.impl.sqlite]))

(def walkable-parser
  (p/async-parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
         [sqb/async-pull-entities p/env-placeholder-reader p/map-reader]})]}))

(defn async-run-query
  [db [q & params]]
  (let [c (promise-chan)]
    (.all db q (or (to-array params) #js [])
      (fn callback [e r]
        (let [x (js->clj r :keywordize-keys true)]
          (put! c x))))
    c))

(def db (sqlite3/Database. "walkable_dev.sqlite"))

(def db-specific-emitter
  {:sqlite emitter/sqlite-emitter})

(defn run-scenario-tests*
  [db db-type scenarios]
  (for [[scenario {:keys [core-floor-plan test-suite]}] scenarios
        {:keys [message env query expected]}            test-suite]
    {:msg      (str "In scenario " scenario " for " db-type ", testing " message)
     :result   (walkable-parser
                 (->> env (merge {::sqb/sql-db             db
                                  ::sqb/run-query          async-run-query
                                  ::p/placeholder-prefixes #{"ph"}
                                  ::sqb/floor-plan
                                  (floor-plan/compile-floor-plan
                                    (merge core-floor-plan
                                      {:emitter (db-specific-emitter db-type)}))}))
                 query)
     :expected expected}))

(defn run-scenario-tests
  [db db-type scenarios]
  (async done
    (go
      (doseq [{:keys [msg result expected]}
              (run-scenario-tests* db :sqlite common-scenarios)]
        (testing msg
          (is (= expected (<! result)))))
      (done))))

(deftest common-scenarios-test
  (run-scenario-tests db :sqlite common-scenarios))
#_
(deftest sqlite-specific-scenarios-test
  (run-scenario-tests db :sqlite {}))
