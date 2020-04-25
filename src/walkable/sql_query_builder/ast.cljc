(ns walkable.sql-query-builder.ast
  (:require [clojure.zip :as z]
            [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [clojure.spec.alpha :as s]))

(defn target-table
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/target-tables
           dispatch-key]))

(defn target-column
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/target-columns
           dispatch-key]))

(defn source-table
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/source-tables
           dispatch-key]))

(defn source-column
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/source-columns
           dispatch-key]))

(defn keyword-type
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/keyword-type
           dispatch-key]))

(defn join-statement
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/join-statements
           dispatch-key]))

(defn compiled-extra-condition
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-extra-conditions
           dispatch-key]))

(defn compiled-ident-condition
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-ident-conditions
           dispatch-key]))

(defn compiled-join-condition
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-join-conditions
           dispatch-key]))

(defn compiled-join-condition-cte
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-join-conditions-cte
           dispatch-key]))

(defn compiled-join-selection
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-join-selection
           dispatch-key]))

(defn compiled-aggregator-selection
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-aggregator-selection
           dispatch-key]))

(defn compiled-group-by
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-group-by
           dispatch-key]))

(defn compiled-having
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-having
           dispatch-key]))

(defn pagination-fallbacks
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/compiled-pagination-fallbacks
           dispatch-key]))

(defn pagination-default-fallbacks
  [floor-plan]
  (get-in floor-plan
          [::floor-plan/compiled-pagination-fallbacks
           'walkable.sql-query-builder.pagination/default-fallbacks]))

(defn return
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/return
           dispatch-key]))

(defn return-async
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan
          [::floor-plan/return-async
           dispatch-key]))

(defn aggregator?
  [floor-plan {:keys [dispatch-key]}]
  (let [aggregators (-> floor-plan
                        ::floor-plan/aggregator-keywords)]
    (contains? aggregators dispatch-key)))

(defn cte?
  [{::floor-plan/keys [cte-keywords]} {:keys [dispatch-key]}]
  (contains? cte-keywords dispatch-key))

(defn cardinality-one?
  [floor-plan {:keys [dispatch-key]}]
  (= :one (get-in floor-plan
                  [::floor-plan/cardinality
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
      ::floor-plan/variable->graph-index))

(defn compiled-variable-getters
  [floor-plan]
  (-> floor-plan
      ::floor-plan/compiled-variable-getters))

(defn compiled-variable-getter-graphs
  [floor-plan]
  (-> floor-plan
      ::floor-plan/compiled-variable-getter-graphs))

(defn process-supplied-condition
  [{::floor-plan/keys [compiled-formulas join-filter-subqueries]}
   ast]
  (let [supplied-condition (get-in ast [:params :filters])]
    (when supplied-condition
      (->> supplied-condition
           (expressions/compile-to-string
            {:join-filter-subqueries join-filter-subqueries})
           (expressions/substitute-atomic-variables
            {:variable-values compiled-formulas})))))

(defn query-dispatch
  [{:keys [aggregator? cte?]} _main-args]
  (mapv boolean [aggregator? cte?]))

(defmulti shared-query query-dispatch)

(defmulti individual-query query-dispatch)

(defmethod shared-query :default
  [& _args])

(defmethod individual-query :default
  [& _args])

(defmethod individual-query
  [false true]
  [_dispatch-values {:keys [floor-plan ast pagination]}]
  (let [selection expressions/select-all
        conditions (compiled-join-condition-cte floor-plan ast)
        {:keys [offset limit order-by]} pagination

        sql-query {:raw-string
                   (emitter/->query-string
                    {:target-table "walkable_ungrouped_children"

                     :selection (:raw-string selection)
                     :conditions (:raw-string conditions)

                     :offset offset
                     :limit limit
                     :order-by order-by})
                   :params (expressions/combine-params selection conditions)}]
    sql-query))

(defn process-children*
  "Infers which columns to include in SQL query from child keys in ast"
  [floor-plan ast] 
  (let [all-children (:children ast)
        
        {:keys [columns joins]}
        (group-by #(keyword-type floor-plan %) all-children)

        child-column-keys
        (into #{} (map :dispatch-key) columns)

        child-source-columns
        (into #{} (map #(source-column floor-plan %)) joins)]
    {:columns-to-query (clojure.set/union child-column-keys child-source-columns)}))

(defn process-children
  "Infers which columns to include in SQL query from child keys in env ast"
  [floor-plan ast]
  (if (aggregator? floor-plan ast)
    {:columns-to-query #{(:dispatch-key ast)}}
    (process-children* floor-plan ast)))

(defn process-selection [floor-plan columns-to-query]
  (let [{::floor-plan/keys [compiled-selection]} floor-plan
        compiled-normal-selection (mapv compiled-selection columns-to-query)]
    (expressions/concat-with-comma compiled-normal-selection)))

(defn all-conditions
  [floor-plan ast]
  (let [conditions
        (->> [(compiled-join-condition floor-plan ast)
              (process-supplied-condition floor-plan ast)
              (compiled-extra-condition floor-plan ast)]
             (into [] (remove nil?)))]
    (expressions/concat-with-and conditions)))

(defn shared-conditions
  [floor-plan ast]
  (let [conditions
        (->> [(process-supplied-condition floor-plan ast)
              (compiled-extra-condition floor-plan ast)]
             (into [] (remove nil?)))]
    (expressions/concat-with-and conditions)))

(defn conj-some [coll x]
  (if x
    (conj coll x)
    coll))

(defmethod individual-query
  [false false]
  [_dispatch {:keys [floor-plan ast pagination]}]
  (let [{:keys [columns-to-query]} (process-children floor-plan ast)
        target-column              (target-column floor-plan ast)
        {:keys [offset limit order-by order-by-columns]} pagination

        columns-to-query
        (-> (clojure.set/union columns-to-query order-by-columns)
            (conj-some target-column))

        selection
        (process-selection floor-plan columns-to-query)

        conditions (all-conditions floor-plan ast)

        having    (compiled-having floor-plan ast)
        sql-query {:raw-string
                   (emitter/->query-string
                    {:target-table   (target-table floor-plan ast)
                     :join-statement (join-statement floor-plan ast)
                     :selection      (:raw-string selection)
                     :conditions     (:raw-string conditions)
                     :group-by       (compiled-group-by floor-plan ast)
                     :having         (:raw-string having)
                     :offset         offset
                     :limit          limit
                     :order-by       order-by})
                   :params (expressions/combine-params selection conditions having)}]
    sql-query))

(defmethod individual-query
  [true true]
  [_dispatch {:keys [floor-plan ast]}]
  (let [selection  (compiled-aggregator-selection floor-plan ast)
        conditions (compiled-join-condition-cte floor-plan ast)

        sql-query {:raw-string
                   (emitter/->query-string
                    {:target-table "walkable_ungrouped_children"
                     :selection    (:raw-string selection)
                     :conditions   (:raw-string conditions)})
                   :params (expressions/combine-params selection conditions)}]
    sql-query))

(defmethod individual-query
  [true false]
  [_dispatch {:keys [floor-plan ast]}]
  (let [selection  (compiled-aggregator-selection floor-plan ast)
        conditions (all-conditions floor-plan ast)
        sql-query  {:raw-string
                    (emitter/->query-string
                     {:target-table   (target-table floor-plan ast)
                      :join-statement (join-statement floor-plan ast)
                      :selection      (:raw-string selection)
                      :conditions     (:raw-string conditions)})
                    :params (expressions/combine-params selection conditions)}]
    sql-query))

(defmethod shared-query
  [false true]
  [_dispatch {:keys [floor-plan ast]}]
  (let [{:keys [columns-to-query]} (process-children floor-plan ast)
        target-column (target-column floor-plan ast)
        {:keys [order-by-columns]} (process-pagination floor-plan ast)
        columns-to-query (-> (clojure.set/union columns-to-query order-by-columns)
                             (conj target-column))
        selection (process-selection floor-plan columns-to-query)
        conditions (shared-conditions floor-plan ast)
        having (compiled-having floor-plan ast)
        sql-query {:raw-string
                   (str "WITH walkable_ungrouped_children AS ("
                        (emitter/->query-string
                         {:target-table (target-table floor-plan ast)
                          :join-statement (join-statement floor-plan ast)
                          :selection (:raw-string selection)
                          :conditions (:raw-string conditions)
                          :group-by (compiled-group-by floor-plan ast)
                          :having (:raw-string having)})
                        ")\n")
                   :params (expressions/combine-params selection conditions having)}]
    sql-query))

(defmethod shared-query
  [true true]
  [_dispatch {:keys [floor-plan ast]}]
  (let [target-column    (target-column floor-plan ast)
        columns-to-query #{target-column}
        selection        (process-selection floor-plan columns-to-query)
        conditions       (shared-conditions floor-plan ast)
        having           (compiled-having floor-plan ast)
        sql-query        {:raw-string
                          (str "WITH walkable_ungrouped_children AS ("
                               (emitter/->query-string
                                {:target-table   (target-table floor-plan ast)
                                 :join-statement (join-statement floor-plan ast)
                                 :selection      (:raw-string selection)
                                 :conditions     (:raw-string conditions)
                                 :group-by       (compiled-group-by floor-plan ast)
                                 :having         (:raw-string having)})
                               ")\n")
                          :params (expressions/combine-params selection conditions having)}]
    sql-query))

(defn ast-zipper
  "Make a zipper to navigate an ast tree."
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

(defn combine-with-cte [{:keys [shared-query batched-individuals]}]
  (expressions/concatenate #(apply str %)
                           [shared-query batched-individuals]))

(defn combine-without-cte [{:keys [batched-individuals]}]
  batched-individuals)

(defn source-column-variable-values
  [v]
  {:variable-values {`floor-plan/source-column-value
                     (expressions/compile-to-string {} v)}})

(defn individual-queries
  [batch-query individual-query source-column-keyword]
  (let [xform (comp (map #(get % source-column-keyword))
                    (remove nil?)
                    (map #(expressions/substitute-atomic-variables
                           (source-column-variable-values %)
                           individual-query)))]
    (fn individual-queries* [entities]
      (->> entities
           (into [] xform)
           batch-query))))

(defn prepare-query
  [floor-plan ast]
  (let [kt (keyword-type floor-plan ast)]
    (when (#{:roots :joins} kt)
      (let [dispatch         {:aggregator? (aggregator? floor-plan ast)
                              :cte?        (cte? floor-plan ast)}
            params           [dispatch {:floor-plan floor-plan
                                        :ast        ast
                                        :pagination (process-pagination floor-plan ast)}]
            
            shared-query     (apply shared-query params)
            individual-query (apply individual-query params)

            batched-individuals
            (if (= :roots kt)
              (constantly individual-query)
              (individual-queries (::floor-plan/batch-query floor-plan)
                                  individual-query
                                  (source-column floor-plan ast)))

            combine-query (if (:cte? dispatch)
                            combine-with-cte
                            combine-without-cte)]
        (fn final-query [entities]
          (combine-query {:shared-query shared-query
                          :batched-individuals (batched-individuals entities)}))))))

(defn prepare-merge-sub-entities
  [floor-plan ast]
  (let [k  (:dispatch-key ast)
        sc (source-column floor-plan ast)]
    (fn merge-sub-entities [entities sub-entities]
      (if (empty? sub-entities)
        entities
        (let [groups (group-by sc sub-entities)]
          (mapv #(let [source-column-value (get % sc)]
                  (assoc % k
                         (get groups source-column-value)))
               entities))))))

(defn prepared-ast
  [floor-plan ast]
  (ast-map (fn [ast-item] (if-let [pq (prepare-query floor-plan ast-item)]
                            (assoc ast-item ::prepared-query pq
                                   ::prepared-merge-sub-entities (prepare-merge-sub-entities floor-plan ast-item))
                            ast-item))
           (ast-zipper ast)))
