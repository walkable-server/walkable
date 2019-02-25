(ns walkable.sql-query-builder.pathom-env
  (:refer-clojure :exclude [key])
  (:require [com.wsscode.pathom.core :as p]))

(defn dispatch-key
  [env]
  (-> env :ast :dispatch-key))

(defn key
  [env]
  (-> env :ast :key))

(defn ident-value
  [env]
  (let [k (key env)]
    (when (vector? k)
      (second k))))

(defn ident-column
  [env]
  (let [ident-columns (-> env :walkable.sql-query-builder/floor-plan
                        :walkable.sql-query-builder.floor-plan/ident-columns)]
    (get ident-columns (dispatch-key env))))

(defn target-column
  [env]
  (let [target-columns (-> env :walkable.sql-query-builder/floor-plan
                         :walkable.sql-query-builder.floor-plan/target-columns)]
    (get target-columns (dispatch-key env))))

(defn target-table
  [env]
  (let [target-tables (-> env :walkable.sql-query-builder/floor-plan
                        :walkable.sql-query-builder.floor-plan/target-tables)]
    (get target-tables (dispatch-key env))))

(defn source-column
  [env]
  (let [source-columns (-> env :walkable.sql-query-builder/floor-plan
                         :walkable.sql-query-builder.floor-plan/source-columns)]
    (get source-columns (dispatch-key env))))

(defn source-column-value
  [env]
  (let [e (p/entity env)]
    (get e (source-column env))))

(defn source-table
  [env]
  (let [source-tables (-> env :walkable.sql-query-builder/floor-plan
                        :walkable.sql-query-builder.floor-plan/source-tables)]
    (get source-tables (dispatch-key env))))

(defn join-statement
  [env]
  (let [join-statements (-> env :walkable.sql-query-builder/floor-plan
                          :walkable.sql-query-builder.floor-plan/join-statements)]
    (get join-statements (dispatch-key env))))

(defn compiled-extra-condition
  [env]
  (let [extra-conditions (-> env :walkable.sql-query-builder/floor-plan
                           :walkable.sql-query-builder.floor-plan/compiled-extra-conditions)]
    (get extra-conditions (dispatch-key env))))

(defn compiled-ident-condition
  [env]
  (let [ident-conditions (-> env :walkable.sql-query-builder/floor-plan
                           :walkable.sql-query-builder.floor-plan/compiled-ident-conditions)]
    (get ident-conditions (dispatch-key env))))

(defn compiled-join-condition
  [env]
  (let [join-conditions (-> env :walkable.sql-query-builder/floor-plan
                          :walkable.sql-query-builder.floor-plan/compiled-join-conditions)]
    (get join-conditions (dispatch-key env))))

(defn compiled-join-condition-cte
  [env]
  (let [join-conditions (-> env :walkable.sql-query-builder/floor-plan
                          :walkable.sql-query-builder.floor-plan/compiled-join-conditions-cte)]
    (get join-conditions (dispatch-key env))))

(defn compiled-join-selection
  [env]
  (let [join-selection (-> env :walkable.sql-query-builder/floor-plan
                         :walkable.sql-query-builder.floor-plan/compiled-join-selection)]
    (get join-selection (dispatch-key env))))

(defn compiled-aggregator-selection
  [env]
  (let [aggregator-selection (-> env :walkable.sql-query-builder/floor-plan
                               :walkable.sql-query-builder.floor-plan/compiled-aggregator-selection)]
    (get aggregator-selection (dispatch-key env))))

(defn compiled-group-by
  [env]
  (let [group-bys (-> env :walkable.sql-query-builder/floor-plan
                    :walkable.sql-query-builder.floor-plan/compiled-group-by)]
    (get group-bys (dispatch-key env))))

(defn compiled-having
  [env]
  (let [havings (-> env :walkable.sql-query-builder/floor-plan
                  :walkable.sql-query-builder.floor-plan/compiled-having)]
    (get havings (dispatch-key env))))

(defn pagination-fallbacks
  [env]
  (let [fallbacks (-> env :walkable.sql-query-builder/floor-plan
                    :walkable.sql-query-builder.floor-plan/compiled-pagination-fallbacks)]
    (get fallbacks (dispatch-key env))))

(defn pagination-default-fallbacks
  [env]
  (get-in env
    [:walkable.sql-query-builder/floor-plan
     :walkable.sql-query-builder.floor-plan/compiled-pagination-fallbacks
     'walkable.sql-query-builder.pagination/default-fallbacks]))

(defn return-or-join
  [env]
  (get-in env
    [:walkable.sql-query-builder/floor-plan
     :walkable.sql-query-builder.floor-plan/return-or-join
     (dispatch-key env)]))

(defn return-or-join-async
  [env]
  (get-in env
    [:walkable.sql-query-builder/floor-plan
     :walkable.sql-query-builder.floor-plan/return-or-join-async
     (dispatch-key env)]))

(defn aggregator?
  [env]
  (let [aggregators (get-in env
                      [:walkable.sql-query-builder/floor-plan
                       :walkable.sql-query-builder.floor-plan/aggregator-keywords])]
    (contains? aggregators (dispatch-key env))))

(defn cardinality-one?
  [env]
  (->> [:walkable.sql-query-builder/floor-plan
        :walkable.sql-query-builder.floor-plan/cardinality
        (dispatch-key env)]
    (get-in env)
    (= :one)))

(defn params [env]
  (get-in env [:ast :params]))

(defn offset [env]
  (when-let [offset (get-in env [:ast :params :offset])]
    (when (integer? offset)
      offset)))

(defn limit [env]
  (when-let [limit (get-in env [:ast :params :limit])]
    (when (integer? limit)
      limit)))

(defn order-by [env]
  (get-in env [:ast :params :order-by]))

(defn variable->graph-index
  [env]
  (-> env :walkable.sql-query-builder/floor-plan
    :walkable.sql-query-builder.floor-plan/variable->graph-index))

(defn compiled-variable-getters
  [env]
  (-> env :walkable.sql-query-builder/floor-plan
    :walkable.sql-query-builder.floor-plan/compiled-variable-getters))

(defn compiled-variable-getter-graphs
  [env]
  (get-in env [:walkable.sql-query-builder/floor-plan
               :walkable.sql-query-builder.floor-plan/compiled-variable-getter-graphs]))
