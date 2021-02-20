(ns walkable.integration-test.helper
  (:require [clojure.java.jdbc :as jdbc]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [walkable.core :as walkable]
            [clojure.test :refer [testing is]]))

(defn walkable-parser
  [db-type registry]
  (p/parser
    {::p/env {::p/reader [p/map-reader
                          pc/reader3
                          pc/open-ident-reader
                          p/env-placeholder-reader]}
     ::p/plugins [(pc/connect-plugin {::pc/register []})
                  (walkable/connect-plugin {:db-type db-type
                                            :registry registry
                                            :query-env #(jdbc/query (:db %1) %2)})
                  p/elide-special-outputs-plugin
                  p/error-handler-plugin
                  p/trace-plugin]}))

(defn run-scenario-tests
  [db db-type scenarios]
  (into []
    (for [[scenario {:keys [:registry :test-suite]}] scenarios
          {:keys [:message :env :query :expected]} test-suite]
      (testing (str "In scenario " scenario " for " db-type ", testing " message)
        (is (= expected
              (let [parser (walkable-parser db-type registry)]
                (parser (assoc env :db db)
                  query))))))))
