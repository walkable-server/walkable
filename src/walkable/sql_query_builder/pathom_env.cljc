(ns walkable.sql-query-builder.pathom-env
  (:refer-clojure :exclude [key])
  (:require [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]))

(defn planner-node [env]
  (-> env
    :com.wsscode.pathom.connect.planner/node))

(defn planner-input [env]
  (-> env
    :com.wsscode.pathom.connect.planner/node
    :com.wsscode.pathom.connect.planner/input))

(defn planner-requires [env]
  (-> env
    :com.wsscode.pathom.connect.planner/node
    :com.wsscode.pathom.connect.planner/requires))

(defn planner-foreign-ast [env]
  (-> env
    :com.wsscode.pathom.connect.planner/node
    :com.wsscode.pathom.connect.planner/foreign-ast))

(defn config
  [env]
  (get-in env [::pc/resolver-data :walkable.core/config]))

(defn floor-plan
  [env]
  (get-in env [::pc/resolver-data :walkable.core/config :walkable.core/floor-plan]))
