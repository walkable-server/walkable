(ns walkable.sql-query-builder.ast
  (:require [clojure.zip :as z]
            [clojure.set :as set]
            [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]))

(defn target-table
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:target-table dispatch-key]))

(defn target-column
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:target-column dispatch-key]))

(defn source-column
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:source-column dispatch-key]))

(defn result-key [ast]
  (let [k (:pathom/as (:params ast))]
    (if (keyword? k)
      k
      (:key ast))))

(defn merge-sub-entities
  [floor-plan {:keys [:dispatch-key]}]
  (get-in floor-plan [:merge-sub-entities dispatch-key]))

(defn query-multiplier
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:query-multiplier dispatch-key]))

(defn keyword-type
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:keyword-type dispatch-key]))

(defn join-statement
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:join-statement dispatch-key]))

(defn compiled-join-selection
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:compiled-join-selection dispatch-key]))

(defn compiled-join-aggregator-selection
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:compiled-join-aggregator-selection dispatch-key]))

(defn compiled-group-by
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:compiled-group-by dispatch-key]))

(defn compiled-having
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:compiled-having dispatch-key]))

(defn pagination-fallbacks
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:compiled-pagination-fallbacks dispatch-key]))

(defn pagination-default-fallbacks
  [floor-plan]
  (get-in floor-plan
          [:compiled-pagination-fallbacks 'walkable.sql-query-builder.pagination/default-fallbacks]))

(defn return
  [floor-plan {:keys [dispatch-key]}]
  (get-in floor-plan [:return dispatch-key]))

(defn aggregator?
  [floor-plan {:keys [dispatch-key]}]
  (let [aggregators (:aggregator-keywords floor-plan)]
    (contains? aggregators dispatch-key)))

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
  "Processes `:offset` `:limit` and `:order-by` if provided in current ast item
  params."
  [ast]
  {:offset   (supplied-offset ast)
   :limit    (supplied-limit ast)
   :order-by (supplied-order-by ast)})

;; TODO: move to floor-plan
(defn process-pagination
  [floor-plan ast]
  (pagination/merge-pagination
    (pagination-default-fallbacks floor-plan)
    (pagination-fallbacks floor-plan ast)
    (supplied-pagination ast)))

(defn variable->graph-index
  [floor-plan]
  (-> floor-plan :variable->graph-index))

(defn compiled-variable-getter
  [floor-plan]
  (-> floor-plan :compiled-variable-getter))

(defn compiled-variable-getter-graph
  [floor-plan]
  (-> floor-plan :compiled-variable-getter-graph))

(defn process-supplied-filter
  [{:keys [compiled-formulas join-filter-subqueries]}
   ast]
  (let [supplied-condition (get-in ast [:params :filters])]
    (when supplied-condition
      (->> supplied-condition
           (expressions/compile-to-string
            {:join-filter-subqueries join-filter-subqueries})
           (expressions/substitute-atomic-variables
            {:variable-values compiled-formulas})))))

(defn all-filters
  [floor-plan {:keys [dispatch-key key] :as ast}]
  (let [k (if (vector? key) key dispatch-key)
        f (get-in floor-plan [:all-filters k])]
    (f (process-supplied-filter floor-plan ast))))

(defn query-dispatch
  [{:keys [aggregator? cte?]} _main-args]
  (mapv boolean [aggregator? cte?]))

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
    {:columns-to-query (set/union child-column-keys child-source-columns)}))

(defn process-children
  "Infers which columns to include in SQL query from child keys in env ast"
  [floor-plan ast]
  (if (aggregator? floor-plan ast)
    {:columns-to-query #{(:dispatch-key ast)}}
    (process-children* floor-plan ast)))

(defn process-selection [floor-plan columns-to-query]
  (let [{:keys [:compiled-selection]} floor-plan
        compiled-normal-selection (mapv compiled-selection columns-to-query)]
    (expressions/concat-with-comma compiled-normal-selection)))

(defn conj-some [coll x]
  (if x
    (conj coll x)
    coll))

(defn individual-query-template-aggregator
  [{:keys [floor-plan ast]}]
  (let [selection  (compiled-join-aggregator-selection floor-plan ast)
        conditions (all-filters floor-plan ast)
        sql-query  {:raw-string
                    (emitter/->query-string
                     {:target-table   (target-table floor-plan ast)
                      :join-statement (join-statement floor-plan ast)
                      :selection      (:raw-string selection)
                      :conditions     (:raw-string conditions)})
                    :params (expressions/combine-params selection conditions)}]
    sql-query))

(defn individual-query-template
  [{:keys [floor-plan ast pagination]}]
  (let [ident? (vector? (:key ast))
        
        {:keys [:columns-to-query]} (process-children floor-plan ast)
        target-column (target-column floor-plan ast)

        {:keys [:offset :limit :order-by :order-by-columns]}
        (when-not ident? pagination)

        columns-to-query
        (-> (clojure.set/union columns-to-query order-by-columns)
          (conj-some target-column))

        selection
        (process-selection floor-plan columns-to-query)

        conditions (all-filters floor-plan ast)

        having (compiled-having floor-plan ast)

        sql-query {:raw-string
                   (emitter/->query-string
                     {:target-table (target-table floor-plan ast)
                      :join-statement (join-statement floor-plan ast)
                      :selection (:raw-string selection)
                      :conditions (:raw-string conditions)
                      :group-by (compiled-group-by floor-plan ast)
                      :having (:raw-string having)
                      :offset offset
                      :limit limit
                      :order-by order-by})
                   :params (expressions/combine-params selection conditions having)}]
    sql-query))

(defn combine-with-cte [{:keys [:shared-query :batched-individuals]}]
  (expressions/concatenate #(apply str %)
                           [shared-query batched-individuals]))

#_(defn combine-without-cte [{:keys [batched-individuals]}]
  batched-individuals)

(defn source-column-variable-values
  [v]
  {:variable-values {`floor-plan/source-column-value
                     (expressions/compile-to-string {} v)}})

(defn compute-graphs [floor-plan env variables]
  (let [variable->graph-index (variable->graph-index floor-plan)
        graph-index->graph    (compiled-variable-getter-graph floor-plan)]
    (into {}
          (comp (map variable->graph-index)
                (remove nil?)
                (distinct)
                (map #(do [% (graph-index->graph %)]))
                (map (fn [[index graph]] [index (graph env)])))
          variables)))

(defn compute-variables
  [floor-plan env {:keys [computed-graphs variables]}]
  (let [getters (select-keys (compiled-variable-getter floor-plan) variables)]
    (into {}
          (map (fn [[k f]]
                 (let [v (f env computed-graphs)]
                   ;; wrap in single-raw-string to feed
                   ;; `expressions/substitute-atomic-variables`
                   [k (expressions/single-raw-string v)])))
          getters)))

(defn process-variables
  [floor-plan env {:keys [variables]}]
  (compute-variables floor-plan
                     env
                     {:computed-graphs {} #_(compute-graphs floor-plan env variables)
                      :variables       variables}))

(defn process-query
  [floor-plan env query]
  (let [values (process-variables floor-plan env
                 {:variables (expressions/find-variables query)})]
    (expressions/substitute-atomic-variables
      {:variable-values values}
      query)))

(defn eliminate-unknown-variables [query]
  (let [remaining-variables (zipmap (expressions/find-variables query)
                              (repeat expressions/conformed-nil))]
    (expressions/substitute-atomic-variables
      {:variable-values remaining-variables}
      query)))

(defn prepare-ident-query
  [floor-plan ast]
  (let [params   {:floor-plan floor-plan
                  :ast        ast
                  :pagination (process-pagination floor-plan ast)}

        individual-query (individual-query-template params)

        batched-individuals
        (fn [_env _entities]
          (expressions/substitute-atomic-variables
            {:variable-values {`floor-plan/ident-value
                               (expressions/compile-to-string {} (second (:key ast)))}}

            individual-query))]
    (fn final-query [env entities]
      (let [q (batched-individuals env entities)]
        (when (not-empty (:raw-string q))
          (->> q
            (process-query floor-plan env)
            eliminate-unknown-variables))))))

(defn prepare-root-query
  [floor-plan ast]
  (let [params   {:floor-plan floor-plan
                  :ast        ast
                  :pagination (process-pagination floor-plan ast)}

        template (individual-query-template params)

        batched-individuals (fn [_env _entities] template)]
    (fn final-query [env entities]
      (let [q (batched-individuals env entities)]
        (when (not-empty (:raw-string q))
          (->> q
            (process-query floor-plan env)
            eliminate-unknown-variables))))))

(defn prepare-join-query
  [floor-plan ast]
  (let [params {:floor-plan floor-plan
                :ast ast
                :pagination (process-pagination floor-plan ast)}
        template (if (aggregator? floor-plan ast)
                   (individual-query-template-aggregator params)
                   (individual-query-template params))

        multiplier (query-multiplier floor-plan ast)
        batched-individuals (multiplier template)]
    (fn final-query [env entities]
      (let [q (batched-individuals env entities)]
        (when (not-empty (:raw-string q))
          (->> q
            (process-query floor-plan env)
            eliminate-unknown-variables))))))

(defn prepare-query
  [floor-plan ast]
  (let [ident? (let [i (:key ast)]
                 (and (vector? i)
                      (contains? (:ident-keywords floor-plan) (first i))))
        kt     (keyword-type floor-plan ast)]
    (when (or ident? (#{:roots :joins} kt))
      (cond
        ident?
        (prepare-ident-query floor-plan ast)

        (= :roots kt)
        (prepare-root-query floor-plan ast)

        :else
        (prepare-join-query floor-plan ast)))))

(defn prepare-merge-sub-entities
  [floor-plan ast]
  (let [m (merge-sub-entities floor-plan ast)]
    (m (result-key ast))))

(defn ast-zipper
  "Make a zipper to navigate an ast tree."
  [ast]
  (->> ast
       (z/zipper
        (fn branch? [x] (and (map? x)
                             (#{:root :join} (:type x))))
        (fn children [x] (:children x))
        (fn make-node [x xs] (assoc x :children (vec xs))))))

(defn mapz [f ast]
  (loop [loc (ast-zipper ast)]
    (if (z/end? loc)
      (z/root loc)
      (recur
       (z/next
        (let [node (z/node loc)]
          (if (= :root (:type node))
            loc
            (z/edit loc f))))))))

(defn filterz [f ast]
  (loop [loc (ast-zipper ast)]
    (if (z/end? loc)
      (z/root loc)
      (recur
       (z/next
        (let [node (z/node loc)]
          (if (f node)
            loc
            (z/next (z/remove loc)))))))))

(defn prepare-ast
  [floor-plan ast]
  (->> ast
       (mapz (fn [ast-item] (if-let [pq (prepare-query floor-plan ast-item)]
                              (-> ast-item
                                  (dissoc :query)
                                  (assoc ::prepared-query pq
                                         ::prepared-merge-sub-entities (prepare-merge-sub-entities floor-plan ast-item)))
                              ast-item)))
       (filterz #(or (= :root (:type %)) (::prepared-query %)))))
