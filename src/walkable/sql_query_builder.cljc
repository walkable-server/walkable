(ns walkable.sql-query-builder
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.ast :as ast]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.sql-query-builder.pathom-env :as env]
            [clojure.spec.alpha :as s]
            [clojure.core.async :as async
             :refer [go <! >! put!]]
            [com.wsscode.pathom.core :as p]))

(defn process-children*
  "Infers which columns to include in SQL query from child keys in env ast"
  [{::keys [floor-plan] :keys [ast] ::p/keys [placeholder-prefixes]
    :as    env}]
  {:pre  [(s/valid? (s/keys :req [::floor-plan/column-keywords ::floor-plan/source-columns]
                      :opt [::floor-plan/required-columns])
            floor-plan)

          (if placeholder-prefixes
            (set? placeholder-prefixes)
            true)]
   :post [#(s/valid? (s/keys :req-un [::join-children ::columns-to-query]) %)]}
  (let [{::floor-plan/keys [column-keywords required-columns source-columns]} floor-plan

        all-children
        (ast/find-all-children ast
          {:placeholder? #(contains? placeholder-prefixes
                            (namespace (:dispatch-key %)))
           :leaf?        #(or ;; it's a column child
                            (contains? column-keywords (:dispatch-key %))
                            ;; it's a join child
                            (contains? source-columns (:dispatch-key %))
                            ;; it's a attr child that requires some other column children
                            (contains? required-columns (:dispatch-key %)))})

        {:keys [column-children join-children]}
        (->> all-children
          (group-by #(cond (contains? source-columns (:dispatch-key %))
                           :join-children

                           (contains? column-keywords (:dispatch-key %))
                           :column-children)))

        all-child-keys
        (->> all-children (map :dispatch-key) (into #{}))

        child-column-keys
        (->> column-children (map :dispatch-key) (into #{}))

        child-required-keys
        (->> all-child-keys (map #(get required-columns %)) (apply clojure.set/union))

        child-join-keys
        (map :dispatch-key join-children)

        child-source-columns
        (->> child-join-keys (map #(get source-columns %)) (into #{}))]
    {:join-children    (set join-children)
     :columns-to-query (clojure.set/union
                         child-column-keys
                         child-required-keys
                         child-source-columns)}))

(defn process-children
  [{::keys [floor-plan] :as env}]
  (let [{::floor-plan/keys [aggregator-keywords]} floor-plan
        k                                         (env/dispatch-key env)]
    (if (contains? aggregator-keywords k)
      {:columns-to-query #{k}
       :join-children    #{}}
      (process-children* env))))

(defn supplied-pagination
  "Processes :offset :limit and :order-by if provided in current
  om.next query params."
  [env]
  {:offset   (env/offset env)
   :limit    (env/limit env)
   :order-by (env/order-by env)})

(defn process-ident-condition
  [{::keys [floor-plan] :as env}]
  (when-let [compiled-ident-condition (env/compiled-ident-condition env)]
    (->> compiled-ident-condition
      (expressions/substitute-atomic-variables
        {:variable-values {`floor-plan/ident-value
                           (expressions/compile-to-string {} (env/ident-value env))}}))))

(defn process-pagination [{::keys [floor-plan] :as env}]
  {:pre  [(s/valid? (s/keys :req [::floor-plan/clojuric-names]) floor-plan)]
   :post [#(s/valid? (s/keys :req-un [::offset ::limit ::order-by ::order-by-columns]) %)]}
  (pagination/merge-pagination
    (env/pagination-default-fallbacks env)
    (env/pagination-fallbacks env)
    (supplied-pagination env)))

(defn process-supplied-condition
  [{::keys [floor-plan] :as env}]
  (let [{::floor-plan/keys [compiled-formulas join-filter-subqueries]}
        floor-plan

        supplied-condition
        (get-in env [:ast :params :filters])]
    (when supplied-condition
      (->> supplied-condition
        (expressions/compile-to-string
          {:join-filter-subqueries join-filter-subqueries})
        (expressions/substitute-atomic-variables
          {:variable-values compiled-formulas})))))

(defn concat-with-and [xs]
  (clojure.string/join " AND "
    (mapv (fn [x] (str "(" x ")")) xs)))

(defn top-level-process-conditions
  [{::keys [floor-plan] :as env}]
  (let [{::floor-plan/keys [compiled-conditions]} floor-plan
        conditions
        (->> env
          ((juxt process-ident-condition
             process-supplied-condition
             env/compiled-extra-condition))
          (into [] (remove nil?)))]
    (when (seq conditions)
      (expressions/concatenate concat-with-and
        conditions))))

(defn child-join-process-conditions
  [{::keys [floor-plan] :as env}]
  (let [{::floor-plan/keys [compiled-conditions]} floor-plan
        conditions
        (->> env
          ((juxt
             env/compiled-join-condition
             process-supplied-condition
             env/compiled-extra-condition))
          (into [] (remove nil?)))]
    (when (seq conditions)
      (expressions/concatenate concat-with-and
        conditions))))

(defn top-level-process-selection
  [{::keys [floor-plan] :as env} columns-to-query]
  (let [{::floor-plan/keys [compiled-selection]} floor-plan

        compiled-normal-selection (mapv compiled-selection columns-to-query)]
    (expressions/concatenate  #(clojure.string/join ", " %)
      compiled-normal-selection)))

(defn child-join-process-selection
  [{::keys [floor-plan] :as env} columns-to-query]
  (let [{::floor-plan/keys [compiled-selection]} floor-plan

        compiled-join-selection   (env/compiled-join-selection env)
        compiled-normal-selection (mapv compiled-selection columns-to-query)
        all-compiled-selection    (if compiled-join-selection
                                    (conj compiled-normal-selection compiled-join-selection)
                                    compiled-normal-selection)]
    (expressions/concatenate  #(clojure.string/join ", " %)
      all-compiled-selection)))

(defn combine-params
  [& compiled-exprs]
  (into [] (comp (map :params) cat)
    compiled-exprs))

(defn top-level-process-query*
  [{::keys [floor-plan] :as env}]
  (let [{:keys [join-children columns-to-query]}
        (process-children env)

        {:keys [offset limit order-by order-by-columns]}
        (process-pagination env)

        columns-to-query (clojure.set/union columns-to-query order-by-columns)
        selection        (top-level-process-selection env columns-to-query)
        conditions       (top-level-process-conditions env)
        having           (env/compiled-having env)
        sql-query        {:raw-string
                          (emitter/->query-string
                            {:target-table   (env/target-table env)
                             :join-statement (env/join-statement env)
                             :selection      (:raw-string selection)
                             :conditions     (:raw-string conditions)
                             :group-by       (env/compiled-group-by env)
                             :having         (:raw-string having)
                             :offset         offset
                             :limit          limit
                             :order-by       order-by})
                          :params (combine-params selection conditions having)}]
    {:sql-query     sql-query
     :join-children join-children}))

(defn child-join-process-query*
  [{::keys [floor-plan] :as env}]
  (let [{:keys [join-children columns-to-query]}
        (process-children env)

        {:keys [offset limit order-by order-by-columns]}
        (process-pagination env)

        columns-to-query (clojure.set/union columns-to-query order-by-columns)
        selection        (process-selection env columns-to-query)
        conditions       (child-join-process-conditions env)
        having           (env/compiled-having env)
        sql-query        {:raw-string
                          (emitter/->query-string
                            {:target-table   (env/target-table env)
                             :join-statement (env/join-statement env)
                             :selection      (:raw-string selection)
                             :conditions     (:raw-string conditions)
                             :group-by       (env/compiled-group-by env)
                             :having         (:raw-string having)
                             :offset         offset
                             :limit          limit
                             :order-by       order-by})
                          :params (combine-params selection conditions having)}]
    {:sql-query     sql-query
     :join-children join-children}))

(defn compute-graphs [env variables]
  (let [variable->graph-index (env/variable->graph-index env)
        graph-index->graph    (env/compiled-variable-getter-graphs env)]
    (into {}
      (comp (map variable->graph-index)
        (remove nil?)
        (distinct)
        (map #(do [% (graph-index->graph %)]))
        (map (fn [[index graph]] [index (graph env)])))
      variables)))

(defn compute-variables
  [env computed-graphs variables]
  (let [getters (select-keys (env/compiled-variable-getters env) variables)]
    (into {}
      (map (fn [[k f]]
             (let [v (f env computed-graphs)]
               ;; wrap in single-raw-string to feed
               ;; `expressions/substitute-atomic-variables`
               [k (expressions/single-raw-string v)])))
      getters)))

(defn process-variables
  [{::keys [floor-plan] :as env} variables]
  (let [computed-graphs
        (compute-graphs env variables)]
    (compute-variables env computed-graphs variables)))

(defn top-level-process-query
  [env]
  (let [query           (top-level-process-query* env)
        sql-query       (:sql-query query)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (assoc query :sql-query
      (expressions/substitute-atomic-variables
        {:variable-values variable-values} sql-query))))

(defn child-join-process-query
  [env]
  (let [query           (child-join-process-query* env)
        sql-query       (:sql-query query)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (assoc query :sql-query
      (expressions/substitute-atomic-variables
        {:variable-values variable-values} sql-query))))

(defn build-parameterized-sql-query
  [{:keys [raw-string params]}]
  (vec (cons raw-string params)))

(defn top-level
  [{::keys [floor-plan sql-db run-query] :as env}]
  (let [{::floor-plan/keys [ident-keywords]}
        floor-plan

        k                                 (env/dispatch-key env)
        {:keys [sql-query join-children]} (top-level-process-query env)]
    {:join-children join-children
     :entities      (if (contains? ident-keywords k)
                      ;; for idents
                      (run-query sql-db (build-parameterized-sql-query sql-query))
                      ;; joins don't have to build a query themselves
                      ;; just look up the key in their parents data
                      (let [parent (p/entity env)]
                        (get parent (:ast env))))}))

(defn top-level-async
  [{::keys [floor-plan sql-db run-query] :as env}]
  (let [{::floor-plan/keys [ident-keywords]}
        floor-plan

        k                                 (env/dispatch-key env)
        {:keys [sql-query join-children]} (top-level-process-query env)]
    (go
      {:join-children join-children
       :entities      (if (contains? ident-keywords k)
                        ;; for idents
                        (<! (run-query sql-db (build-parameterized-sql-query sql-query)))
                        ;; joins don't have to build a query themselves
                        ;; just look up the key in their parents data
                        (let [parent (p/entity env)]
                          (get parent (:ast env))))})))

(defn source-column-variable-values
  [v]
  {:variable-values {`floor-plan/source-column-value
                     (expressions/verbatim-raw-string v)}})

(defn join-children-data
  [{::keys [floor-plan] :as env}
   entities join-children]
  (let [{::floor-plan/keys [batch-query
                            target-columns
                            source-columns]} floor-plan]
    (when (and (seq entities) (seq join-children))
      (let [f (fn [join-child]
                (let [j             (:dispatch-key join-child)
                      ;; parent
                      source-column (get source-columns j)
                      ;; children
                      target-column (get target-columns j)

                      unbound-sql-query
                      (:sql-query (child-join-process-query (assoc env :ast join-child)))

                      queries
                      (for [e    entities
                            :let [v (get e source-column)]]
                        (->> unbound-sql-query
                          (expressions/substitute-atomic-variables
                            (source-column-variable-values v))))]
                  [join-child
                   {:data-fn #(group-by target-column %)
                    :query   (build-parameterized-sql-query (batch-query queries))}]))]
        (mapv f join-children)))))

(defn join-children-data-by-join-key
  [{::keys [run-query sql-db] :as env} entities join-children]
  (let [f (fn [[join-child {:keys [data-fn query]}]]
            (let [data (run-query sql-db query)]
              [join-child (data-fn data)]))]
    (into {}
      (map f)
      (join-children-data env entities join-children))))

(defn join-childen-data-by-join-key-async
  [{::keys [run-query sql-db] :as env} entities join-children]
  (async/into {}
    (async/merge
      (map (fn [[join-child {:keys [data-fn query]}]]
             (go (let [data (<! (run-query sql-db query))]
                  [join-child (data-fn data)])))
        (join-children-data env entities join-children)))))

(defn entities-with-join-children-data
  [join-children-data-by-join-key entities source-columns join-children]
  (for [e entities]
    (let [f (fn [join-child]
              (let [j             (:dispatch-key join-child)
                    source-column (get source-columns j)
                    parent-id     (get e source-column)
                    children      (get-in join-children-data-by-join-key
                                    [join-child parent-id])]
                [join-child children]))
          child-joins (into {} (map f) join-children)]
      (merge e child-joins))))

(defn pull-entities
  "A Pathom plugin that pulls entities from SQL database and puts
  relevent data to ::p/entity ready for p/map-reader plugin.

  The env given to the Pathom parser must contains:

  - floor-plan: output of compile-floor-plan

  - sql-db: a database instance

  - run-query: a function that run an SQL query (optionally with
  params) against the given sql-db. Shares the same signature with
  clojure.java.jdbc/query."
  [{::keys [floor-plan] :as env}]
  (let [{::floor-plan/keys [target-tables source-columns]}
        floor-plan

        k (env/dispatch-key env)]
    (if (contains? target-tables k)
      ;; this is an ident or a join, let's go for data
      (let [{:keys [join-children entities]} (top-level env)

            full-entities
            (-> (join-children-data-by-join-key env entities join-children)
              (entities-with-join-children-data entities source-columns join-children))

            return-or-join
            (env/return-or-join env)

            one?
            (env/cardinality-one? env)]
        (if (seq full-entities)
          (return-or-join env full-entities)
          (if-not one?
            []
            {})))

      ::p/continue)))

(defn async-pull-entities
  [{::keys [floor-plan] :as env}]
  (let [{::floor-plan/keys [target-tables source-columns]}
        floor-plan

        k         (env/dispatch-key env)]
    (if (contains? target-tables k)
      (go (let [{:keys [join-children entities]} (<! (top-level-async env))

                full-entities
                (-> (<! (join-childen-data-by-join-key-async env entities join-children))
                  (entities-with-join-children-data entities source-columns join-children))

                return-or-join-async
                (env/return-or-join-async env)

                one?
                (env/cardinality-one? env)]
            (if (seq full-entities)
              (<! (return-or-join-async env full-entities))
              (if one?
                {}
                []))))

      ::p/continue)))
