(ns walkable.core-async
  (:require [walkable.core :as core]
            [clojure.core.async :as async :refer [go <! >!]]))

(defn wrap-merge
  [f]
  (fn wrapped-merge [entities sub-entites]
    (let [ch (async/promise-chan)]
      (go (let [e (<! entities)
                se (<! sub-entites)]
            (>! ch (f e se))))
      ch)))

(defn ast-resolver
  [floor-plan env ast]
  (core/ast-resolver* floor-plan env wrap-merge ast))

(defn prepared-ast-resolver
  [env prepared-ast]
  (core/prepared-ast-resolver* env wrap-merge prepared-ast))

(defn query-resolver
  [floor-plan env query]
  (core/query-resolver* floor-plan env ast-resolver query))

(defn dynamic-resolver
  [floor-plan env]
  (let [i (ident env)
        ast (-> env ::pcp/node ::pcp/foreign-ast
                (wrap-with-ident i))
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
