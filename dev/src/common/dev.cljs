(ns dev
  (:require [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [walkable.core-async :as walkable]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.integration-test.common :as common]
            ["sqlite3" :as sqlite3]
            [walkable.sql-query-builder.impl.sqlite]
            [cljs.core.async :as async :refer [put! >! <! promise-chan]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(enable-console-print!)

(defn connect-config
  [{:keys [core-config core-floor-plan cte?]}]
  (let [resolver-sym `test-resolver]
    (merge core-config
           {:resolver-sym resolver-sym
            :floor-plan
            (merge core-floor-plan
                   {:emitter emitter/sqlite-emitter
                    :use-cte (when cte? {:default true})})})))

(defn walkable-parser
  [config]
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
                 (walkable/connect-plugin (connect-config config))
                 p/elide-special-outputs-plugin
                 p/error-handler-plugin
                 p/trace-plugin]}))

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

(def common-env
  {::walkable/db db ::walkable/run async-run-print-query})

;; all queries have been comment out
;; uncomment them one by one to see them in action

(let [q-1
      '[{#_ :farmers/farmers
         [:farmer/number 1]
         [:farmer/number :farmer/name
          {:farmer/house [:house/index :house/color]}]}]

      parser
      (walkable-parser {:core-floor-plan common/farmer-house-floor-plan
                        :core-config common/farmer-house-config})]
  (go
    (println "\n\n<-- final result -->\n"
             (<! (parser common-env q-1)))))

(let [q-2
      '[{[:kid/number 1] [:kid/number :kid/name
                         {:kid/toy [:toy/index :toy/color]}]}]

      parser (walkable-parser {:core-floor-plan common/kid-toy-floor-plan
                               :core-config common/kid-toy-config})]
  (println "\n\n")
  (go
    (println "final result"
             (<! (parser common-env q-2)))))

(defn main []
  (println "<-- Walkable for nodejs demo -->"))
