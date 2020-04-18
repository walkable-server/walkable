(ns walkable.sql-query-builder.ast
  (:require [clojure.zip :as z]
            [walkable.sql-query-builder.pagination :as pagination]
            [clojure.spec.alpha :as s]))

(defn target-table
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/target-tables
           dispatch-key]))

(defn target-column
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/target-columns
           dispatch-key]))

(defn source-table
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/source-tables
           dispatch-key]))

(defn source-column
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/source-columns
           dispatch-key]))

(defn join-statement
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/join-statements
           dispatch-key]))

(defn compiled-extra-condition
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-extra-conditions
           dispatch-key]))

(defn compiled-ident-condition
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-ident-conditions
           dispatch-key]))

(defn compiled-join-condition
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-join-conditions
           dispatch-key]))

(defn compiled-join-condition-cte
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-join-conditions-cte
           dispatch-key]))

(defn compiled-join-selection
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-join-selection
           dispatch-key]))

(defn compiled-aggregator-selection
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-aggregator-selection
           dispatch-key]))

(defn compiled-group-by
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-group-by
           dispatch-key]))

(defn compiled-having
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-having
           dispatch-key]))

(defn pagination-fallbacks
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-pagination-fallbacks
           dispatch-key]))

(defn pagination-default-fallbacks
  [floor-plan]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/compiled-pagination-fallbacks
           'walkable.sql-query-builder.pagination/default-fallbacks]))

(defn return
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/return
           dispatch-key]))

(defn return-async
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [:walkable.sql-query-builder.floor-plan/return-async
           dispatch-key]))

(defn aggregator?
  [floor-plan {:keys [dispatch-key]}]
  (let [aggregators (-> floor-plan
                        :walkable.sql-query-builder.floor-plan/aggregator-keywords)]
    (contains? aggregators dispatch-key)))

(defn cardinality-one?
  [floor-plan {:keys [dispatch-key]}]
  (= :one (get-in floor-plan
                  [:walkable.sql-query-builder.floor-plan/cardinality
                   dispatch-key])))

(defn supplied-offset [ast]
  (when-let [offset (get-in ast [:params :offset])]
    (when (integer? offset)
      offset)))

(defn supplied-limit [ast]
  (when-let [limit (get-in ast [:params :limit])]
    (when (integer? limit)
      limit)))

(defn supplied-order-by [ast]
  (get-in ast [:params :order-by]))

(defn supplied-pagination
  "Processes :offset :limit and :order-by if provided in current
   ast item params."
  [ast]
  {:offset   (supplied-offset ast)
   :limit    (supplied-limit ast)
   :order-by (supplied-order-by ast)})

(defn process-pagination
  [floor-plan ast]
  (pagination/merge-pagination
    (pagination-default-fallbacks floor-plan)
    (pagination-fallbacks floor-plan ast)
    (supplied-pagination ast)))

(defn variable->graph-index
  [floor-plan]
  (-> floor-plan
      :walkable.sql-query-builder.floor-plan/variable->graph-index))

(defn compiled-variable-getters
  [floor-plan]
  (-> floor-plan
      :walkable.sql-query-builder.floor-plan/compiled-variable-getters))

(defn compiled-variable-getter-graphs
  [floor-plan]
  (-> floor-plan
      :walkable.sql-query-builder.floor-plan/compiled-variable-getter-graphs))

(defn ast-zipper
  "Make a zipper to navigate an ast tree possibly with placeholder
  subtrees."
  [ast]
  (->> ast
       (z/zipper
        (fn branch? [x] (and (map? x)
                             (#{:root :join} (:type x))))
        (fn children [x] (:children x))
        (fn make-node [x xs] (assoc x :children (vec xs))))))

(defn ast-map [f ast]
  (loop [loc ast]
    (if (z/end? loc)
      (z/root loc)
      (recur
       (z/next
        (let [node (z/node loc)]
          (if (= :root (:type node))
            loc
            (z/edit loc f))))))))

(defn prepare-query
  [floor-plan ast-item])

(defn prepared-ast
  [floor-plan ast]
  (ast-map (fn [ast-item] (assoc ast-item ::prepared-query (prepare-query floor-plan ast-item)))
           (ast-zipper ast)))
