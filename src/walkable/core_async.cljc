(ns walkable.core-async
  (:require [walkable.core :as core]
            [walkable.sql-query-builder.expressions :as expressions]
            [com.wsscode.pathom.connect.planner :as pcp]
            [clojure.core.async :as async :refer [go <! >!]]))

(defn wrap-merge
  [f]
  (fn wrapped-merge [entities sub-entites]
    (let [ch (async/promise-chan)]
      (go (let [e (when entities (<! entities))
                se (when sub-entites (<! sub-entites))]
            (>! ch (f e se))))
      ch)))

(comment
  (go (println (<! ((wrap-merge concat) (go [1 2]) (go [3 4]))))))

(defn build-and-run-query
  [env entities prepared-query]
  (let [ch (async/promise-chan)]
    (go
      (let [q (->> (when entities (<! entities))
                   (prepared-query env)
                   (expressions/build-parameterized-sql-query))
            result (<! ((::run env) (::db env) q))]
        (>! ch result)))
    ch))

(defn ast-resolver
  [floor-plan env ast]
  (core/ast-resolver* {:floor-plan floor-plan
                       :build-and-run-query build-and-run-query
                       :env env
                       :wrap-merge wrap-merge
                       :ast ast}))

(defn prepared-ast-resolver
  [env prepared-ast]
  (core/prepared-ast-resolver* {:env env
                                :build-and-run-query build-and-run-query
                                :wrap-merge wrap-merge
                                :prepared-ast prepared-ast}))

(defn query-resolver
  [floor-plan env query]
  (core/query-resolver* {:floor-plan floor-plan
                         :build-and-run-query build-and-run-query
                         :env env
                         :resolver ast-resolver
                         :query query}))

(defn dynamic-resolver
  [floor-plan env]
  (let [i (core/ident env)
        ast (-> env ::pcp/node ::pcp/foreign-ast
                (core/wrap-with-ident i))
        result (ast-resolver floor-plan env ast)]
    (if i
      (let [ch (async/promise-chan)]
        (go (>! ch (get (<! result) i)))
        ch)
      result)))

(defn connect-plugin
  [{:keys [resolver resolver-sym]
    :or {resolver dynamic-resolver
         resolver-sym `walkable-resolver-async}
    :as config}]
  (core/connect-plugin (assoc config :resolver resolver :resolver-sym resolver-sym)))
