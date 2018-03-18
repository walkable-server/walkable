(ns walkable.sql-query-builder.pathom-env
  (:require [com.wsscode.pathom.core :as p]))

(defn dispatch-key
  [env]
  (-> env :ast :dispatch-key))

(defn target-column
  [env]
  (let [target-columns (-> env :walkable.sql-query-builder/sql-schema :walkable.sql-query-builder/target-columns)]
    (get target-columns (dispatch-key env))))

(defn target-table
  [env]
  (let [target-tables (-> env :walkable.sql-query-builder/sql-schema :walkable.sql-query-builder/target-tables)]
    (get target-tables (dispatch-key env))))

(defn source-column
  [env]
  (let [source-columns (-> env :walkable.sql-query-builder/sql-schema :walkable.sql-query-builder/source-columns)]
    (get source-columns (dispatch-key env))))

(defn source-column-value
  [env]
  (let [e (p/entity env)]
    (get e (source-column env))))

(defn source-table
  [env]
  (let [source-tables (-> env :walkable.sql-query-builder/sql-schema :walkable.sql-query-builder/source-tables)]
    (get source-tables (dispatch-key env))))

(defn join-statement
  [env]
  (let [join-statements (-> env :walkable.sql-query-builder/sql-schema :walkable.sql-query-builder/join-statements)]
    (get join-statements (dispatch-key env))))

(defn extra-condition
  [env]
  (let [extra-conditions (-> env :walkable.sql-query-builder/sql-schema :walkable.sql-query-builder/extra-conditions)]
    (when-let [->condition (get extra-conditions (dispatch-key env))]
      (->condition env))))
