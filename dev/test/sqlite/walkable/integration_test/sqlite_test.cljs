(ns walkable.integration-test.sqlite-test
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require [walkable.integration-test.common :refer [common-scenarios]]
            [cljs.core.async :as async :refer [put! <! promise-chan]]
            [clojure.test :as t :refer [deftest testing is]]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [walkable.core-async :as walkable]
            ["sqlite3" :as sqlite3]))

(defn async-run-query
  [db [q & params]]
  (let [c (promise-chan)]
    (.all db q (or (to-array params) #js [])
      (fn callback [e r]
        (let [x (js->clj r :keywordize-keys true)]
          (put! c x))))
    c))

(defn walkable-parser
  [db-type registry]
  (p/async-parser
    {::p/env {::p/reader [p/map-reader
                          pc/reader3
                          pc/open-ident-reader
                          p/env-placeholder-reader]}
     ::p/plugins [(pc/connect-plugin {::pc/register []})
                  (walkable/connect-plugin {:db-type db-type
                                            :registry registry
                                            :query-env #(async-run-query (:sqlite-db %1) %2)})
                  p/elide-special-outputs-plugin
                  p/error-handler-plugin
                  p/trace-plugin]}))

(def db (sqlite3/Database. "walkable_dev.sqlite"))

(defn run-scenario-tests*
  [db db-type scenarios]
  (for [[scenario {:keys [:registry :test-suite]}] scenarios
        {:keys [:message :env :query :expected]} test-suite]
    {:msg (str "In scenario " scenario " for " db-type ", testing " message) 
     :expected expected
     :result
     (let [parser (walkable-parser db-type registry)]
       (parser (assoc env :sqlite-db db)
         query))}))

(defn run-scenario-tests
  [db db-type scenarios]
  (t/async done
           (go
             (doseq [{:keys [:msg :expected :result]}
                     (run-scenario-tests* db db-type scenarios)]
               (testing msg
                 (is (= expected (<! result)))))
             (done))))

(deftest common-scenarios-test
  (run-scenario-tests db :sqlite common-scenarios))
#_
(deftest sqlite-specific-scenarios-test
  (run-scenario-tests db :sqlite {}))
