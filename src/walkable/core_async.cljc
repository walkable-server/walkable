(ns walkable.core-async
  (:require [walkable.core :as core]
            [clojure.core.async :as async :refer [go go-loop <! >! put!]]))

(defn wrap-merge
  [f]
  (fn wrapped-merge [entities sub-entites]
    (go (f (<! entities) (<! sub-entites)))))

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
  (core/dynamic-resolver* ast-resolver floor-plan env))

(defn connect-plugin
  [{:keys [resolver resolver-sym]
    :or {resolver dynamic-resolver
         resolver-sym `walkable-resolver-async}
    :as config}]
  (core/connect-plugin (assoc config :resolver resolver :resolver-sym resolver-sym)))
