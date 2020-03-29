(ns walkable.sql-query-builder.pathom-env
  (:refer-clojure :exclude [key])
  (:require [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]))

(defn dispatch-key
  [env]
  (-> env :ast :dispatch-key))

(defn key
  [env]
  (-> env :ast :key))

(defn parent-path
  [env]
  (loop [path (pop (::p/path env))]
    (let [k (peek path)]
      (if (or (p/placeholder-key? env k) (number? k))
        (recur (pop path))
        path))))

(comment
  (parent-path {::p/path                 [:a :b 1 :</x :u]
                ::p/placeholder-prefixes #{"<"}}))

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

(defn root-keyword [env]
  (let [{:walkable.sql-query-builder.floor-plan/keys [root-keywords]} (floor-plan env)
        k                                                             (dispatch-key env)]
    (when (contains? root-keywords k)
      k)))

(defn join-keyword [env]
  (let [{:walkable.sql-query-builder.floor-plan/keys [join-keywords]} (floor-plan env)
        k                                                             (dispatch-key env)]
    (when (contains? join-keywords k)
      k)))

(defn ident-keyword [env]
  (let [i (planner-input env)]
    (when (not-empty i)
      (ffirst i))))

(defn ident-value
  [env]
  (let [k (ident-keyword env)
        e (p/entity env)]
    (get e k)))

(defn target-column
  [env]
  (let [target-columns (-> env floor-plan
                         :walkable.sql-query-builder.floor-plan/target-columns)]
    (get target-columns (or (root-keyword env) (join-keyword env) (ident-keyword env)))))

(defn target-table
  [env]
  (let [target-tables (-> env floor-plan
                        :walkable.sql-query-builder.floor-plan/target-tables)
        out (get target-tables (or (root-keyword env) (join-keyword env) (ident-keyword env)))]
    out))

(defn source-column
  [env]
  (let [source-columns (-> env floor-plan
                         :walkable.sql-query-builder.floor-plan/source-columns)]
    (get source-columns (or (root-keyword env) (join-keyword env) (ident-keyword env)))))

(defn source-column-value
  [env]
  (let [e (p/entity env)]
    (get e (source-column env))))

(defn source-table
  [env]
  (let [source-tables (-> env floor-plan
                        :walkable.sql-query-builder.floor-plan/source-tables)]
    (get source-tables (dispatch-key env))))

(defn join-statement
  [env]
  (let [join-statements (-> env floor-plan
                          :walkable.sql-query-builder.floor-plan/join-statements)]
    (get join-statements (dispatch-key env))))

(defn compiled-extra-condition
  [env]
  (let [extra-conditions (-> env floor-plan
                           :walkable.sql-query-builder.floor-plan/compiled-extra-conditions)]
    (get extra-conditions (dispatch-key env))))

(defn compiled-ident-condition
  [env]
  (let [ident-conditions (-> env floor-plan
                           :walkable.sql-query-builder.floor-plan/compiled-ident-conditions)]
    (get ident-conditions (ident-keyword env))))

(defn compiled-join-condition
  [env]
  (let [join-conditions (-> env floor-plan
                          :walkable.sql-query-builder.floor-plan/compiled-join-conditions)]
    (get join-conditions (dispatch-key env))))

(defn compiled-join-condition-cte
  [env]
  (let [join-conditions (-> env floor-plan
                          :walkable.sql-query-builder.floor-plan/compiled-join-conditions-cte)]
    (get join-conditions (dispatch-key env))))

(defn compiled-join-selection
  [env]
  (let [join-selection (-> env floor-plan
                         :walkable.sql-query-builder.floor-plan/compiled-join-selection)]
    (get join-selection (dispatch-key env))))

(defn compiled-aggregator-selection
  [env]
  (let [aggregator-selection (-> env floor-plan
                               :walkable.sql-query-builder.floor-plan/compiled-aggregator-selection)]
    (get aggregator-selection (dispatch-key env))))

(defn compiled-group-by
  [env]
  (let [group-bys (-> env floor-plan
                    :walkable.sql-query-builder.floor-plan/compiled-group-by)]
    (get group-bys (dispatch-key env))))

(defn compiled-having
  [env]
  (let [havings (-> env floor-plan
                  :walkable.sql-query-builder.floor-plan/compiled-having)]
    (get havings (dispatch-key env))))

(defn pagination-fallbacks
  [env]
  (let [fallbacks (-> env floor-plan
                    :walkable.sql-query-builder.floor-plan/compiled-pagination-fallbacks)]
    (get fallbacks (dispatch-key env))))

(defn pagination-default-fallbacks
  [env]
  (-> env
    floor-plan
    (get-in
      [:walkable.sql-query-builder.floor-plan/compiled-pagination-fallbacks
       'walkable.sql-query-builder.pagination/default-fallbacks])))

(defn return
  [env]
  (-> env
    floor-plan
    (get-in
      [:walkable.sql-query-builder.floor-plan/return
       (dispatch-key env)])))

(defn return-async
  [env]
  (-> env
    floor-plan
    (get-in
      [:walkable.sql-query-builder.floor-plan/return-async
       (dispatch-key env)])))

(defn aggregator?
  [env]
  (let [aggregators (-> env floor-plan
                      :walkable.sql-query-builder.floor-plan/aggregator-keywords)]
    (contains? aggregators (dispatch-key env))))

(defn cardinality-one?
  [env]
  (->> (get-in (floor-plan env)
         [:walkable.sql-query-builder.floor-plan/cardinality
          (dispatch-key env)])
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
  (-> env floor-plan
    :walkable.sql-query-builder.floor-plan/variable->graph-index))

(defn compiled-variable-getters
  [env]
  (-> env floor-plan
    :walkable.sql-query-builder.floor-plan/compiled-variable-getters))

(defn compiled-variable-getter-graphs
  [env]
  (-> env floor-plan
    :walkable.sql-query-builder.floor-plan/compiled-variable-getter-graphs))
