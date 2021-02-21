(ns dev
  (:require [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.connect.planner :as pcp]
            [walkable.core-async :as walkable]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.integration-test.common :as common]
            ["sqlite3" :as sqlite3]
            [cljs.core.async :as async :refer [put! >! <! promise-chan]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(enable-console-print!)

(def db (sqlite3/Database. "walkable_dev.sqlite"))

(defn async-run-print-query
  [db [q & params]]
  (let [c (promise-chan)]
    (.all db q (or (to-array params) #js [])
      (fn callback [e r]
        (let [x (js->clj r :keywordize-keys true)]
          (println "\nsql query: " q)
          (println "sql params: " params)
          (println "sql results:" x)
          (put! c x))))
    c))

(defn walkable-parser
  [db-type registry]
  (p/async-parser
   {::p/env {::p/reader [p/map-reader
                         pc/reader3
                         pc/open-ident-reader
                         p/env-placeholder-reader]
             ::p/process-error
             (fn [_ err]
               (js/console.error err)
               (p/error-str err))}
    ::p/plugins [(pc/connect-plugin {::pc/register []})
                 (walkable/connect-plugin {:db-type db-type
                                           :registry registry
                                           :query-env #(async-run-print-query (:db %1) %2)})
                 p/elide-special-outputs-plugin
                 p/error-handler-plugin
                 p/trace-plugin]}))

(def w* (walkable-parser :sqlite common/person-pet-registry))

(defn w
  [q]
  (w* {:db db} q))

(comment
  (w `[{(:pets/by-color {:order-by [:pet/color :desc]})
        [:pet/color]}])

  (w `[(:people/count {:filter [:and {:person/pet [:or [:= :pet/color "white"]
                                                   [:= :pet/color "yellow"]]}
                                [:< :person/number 10]]})]))

(defn main []
  (println "walkable runs in nodejs!!!"))
