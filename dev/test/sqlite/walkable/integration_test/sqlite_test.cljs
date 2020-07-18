(ns walkable.integration-test.sqlite-test
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [walkable.integration-test.common :refer [common-scenarios]]
            [cljs.core.async :as async :refer [put! <! promise-chan]]
            [clojure.test :as t :refer [deftest testing is]]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [walkable.core-async :as walkable]
            [walkable.sql-query-builder.emitter :as emitter]
            ["sqlite3" :as sqlite3]
            [walkable.sql-query-builder.impl.sqlite]))

(def db-specific-emitter
  {:sqlite emitter/sqlite-emitter})

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
  [config]
  (p/async-parser
   {::p/env {::p/reader [p/map-reader
                         pc/reader3
                         pc/open-ident-reader
                         p/env-placeholder-reader]}
    ::p/plugins [(pc/connect-plugin {::pc/register []})
                 (walkable/connect-plugin (connect-config config))
                 p/elide-special-outputs-plugin
                 p/error-handler-plugin
                 p/trace-plugin]}))

(defn async-run-query
  [db [q & params]]
  (let [c (promise-chan)]
    (.all db q (or (to-array params) #js [])
      (fn callback [e r]
        (let [x (js->clj r :keywordize-keys true)]
          (put! c x))))
    c))

(def db (sqlite3/Database. "walkable_dev.sqlite"))

(defn run-scenario-tests*
  [db db-type scenarios]
  (apply concat
    (for [[scenario {:keys [core-floor-plan core-config test-suite]}] scenarios
          {:keys [message env query expected]}            test-suite]
      [{:msg      (str "In scenario " scenario " for " db-type ", testing "
                    message " without CTEs in joins")
        :result   (let [parser (walkable-parser {:core-config core-config
                                                 :core-floor-plan core-floor-plan
                                                 :db-type db-type})]
                    (parser (assoc env ::walkable/db db ::walkable/run async-run-query)
                            query))
        :expected expected}
       {:msg      (str "In scenario " scenario " for " db-type ", testing "
                    message " with CTEs in joins")
        :result   (let [parser (walkable-parser {:core-config core-config
                                                 :core-floor-plan core-floor-plan
                                                 :db-type db-type
                                                 :cte? true})]
                    (parser (assoc env ::walkable/db db ::walkable/run async-run-query)
                            query))
        :expected expected}])))

(defn run-scenario-tests
  [db db-type scenarios]
  (t/async done
           (go
             (doseq [{:keys [msg result expected]}
                     (run-scenario-tests* db db-type scenarios)]
               (testing msg
                 (is (= expected (<! result)))))
             (done))))

(deftest common-scenarios-test
  (run-scenario-tests db :sqlite common-scenarios))
#_
(deftest sqlite-specific-scenarios-test
  (run-scenario-tests db :sqlite {}))
