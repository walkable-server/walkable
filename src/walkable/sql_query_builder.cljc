(ns walkable.sql-query-builder
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.ast :as ast]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.sql-query-builder.pathom-env :as env]
            [clojure.spec.alpha :as s]
            [clojure.core.async :refer [go go-loop <! >! put! promise-chan to-chan]]
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

(defn clean-up-all-conditions
  "Receives all-conditions produced by process-conditions. Only keeps
  non-empty conditions."
  [all-conditions]
  (let [all-conditions (remove nil? all-conditions)]
    (case (count all-conditions)
      0 nil
      1 (first all-conditions)
      (vec all-conditions))))

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
  (pagination/process-pagination
    (::floor-plan/clojuric-names floor-plan)
    (supplied-pagination env)
    (env/pagination-fallbacks env)))

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

(defn process-conditions
  [{::keys [floor-plan] :as env}]
  (let [{::floor-plan/keys [compiled-conditions]} floor-plan
        conditions
        (->> env
          ((juxt process-ident-condition
             env/compiled-join-condition
             process-supplied-condition
             env/compiled-extra-condition))
          (into [] (remove nil?)))]
    (expressions/concatenate #(clojure.string/join " AND " %)
      conditions)))

(defn process-selection
  [{::keys [floor-plan] :as env} columns-to-query]
  (let [{::floor-plan/keys [compiled-selection]} floor-plan]
    (expressions/concatenate  #(clojure.string/join ", " %)
      (mapv compiled-selection columns-to-query))))

(defn combine-params
  [selection conditions]
  (if conditions
    (vec (concat (:params selection) (:params conditions)))
    (:params selection)))

(defn process-query
  [{::keys [floor-plan] :as env}]
  (let [{:keys [join-children columns-to-query]}
        (process-children env)

        {:keys [offset limit order-by order-by-columns]}
        (process-pagination env)

        columns-to-query (clojure.set/union columns-to-query order-by-columns)
        selection        (process-selection env columns-to-query)
        conditions       (process-conditions env)
        sql-query        {:raw-string
                          (emitter/->query-string
                            {:target-table   (env/target-table env)
                             :join-statement (env/join-statement env)
                             :selection      (:raw-string selection)
                             :conditions     (:raw-string conditions)
                             :offset         offset
                             :limit          limit
                             :order-by       order-by})
                          :params (combine-params selection conditions)}]
    {:sql-query     sql-query
     :join-children join-children}))

(defn build-parameterized-sql-query
  [{:keys [raw-string params]}]
  (vec (cons raw-string params)))

(defn top-level
  [{::keys [floor-plan sql-db run-query] :as env}]
  (let [{::floor-plan/keys [ident-keywords join-keywords]}
        floor-plan

        k                                 (env/dispatch-key env)
        {:keys [sql-query join-children]} (process-query env)]
    {:join-children join-children
     :entities      (if (contains? ident-keywords k)
                      ;; for idents
                      (run-query sql-db (build-parameterized-sql-query sql-query))
                      ;; joins don't have to build a query themselves
                      ;; just look up the key in their parents data
                      (let [parent (p/entity env)]
                        (get parent (:ast env))))}))

(defn join-children-data-by-join-key
  [{::keys [floor-plan sql-db run-query] :as env}
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
                      (:sql-query (process-query (assoc env :ast join-child)))

                      queries
                      (for [e    entities
                            :let [v (get e source-column)]]
                        (->> unbound-sql-query
                          (expressions/substitute-atomic-variables
                            {:variable-values {`floor-plan/source-column-value (expressions/verbatim-raw-string v)}})))

                      join-children-data
                      (run-query sql-db (build-parameterized-sql-query (batch-query queries)))]
                  [join-child (group-by target-column join-children-data)]))]
        (into {} (map f) join-children)))))

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
  [{::keys [floor-plan sql-db run-query] :as env}]
  (let [{::floor-plan/keys [target-tables
                            aggregator-keywords
                            source-columns
                            cardinality]} floor-plan
        k                                 (env/dispatch-key env)]
    (if (contains? target-tables k)
      ;; this is an ident or a join, let's go for data
      (let [{:keys [join-children entities]} (top-level env)

            full-entities
            (-> (join-children-data-by-join-key env entities join-children)
              (entities-with-join-children-data entities source-columns join-children))

            one?
            (= :one (get cardinality k))

            do-join
            (if (contains? aggregator-keywords k)
              #(get (first %2) k)
              (if one?
                #(p/join (first %2) %1)
                #(p/join-seq %1 %2)))]
        (if (seq full-entities)
          (do-join env full-entities)
          (when-not one?
            [])))

      ::p/continue)))

(defn async-pull-entities
  "An async Pathom plugin that pulls entities from SQL database and
  puts relevent data to ::p/entity ready for p/map-reader plugin.

  The env given to the Pathom parser must contains:
  - floor-plan: output of compile-floor-plan
  - sql-db: a database instance
  - run-query: a function that run an SQL query (optionally with
  params) against the given sql-db. Shares the same input with
  clojure.java.jdbc/query. Returns query result in a channel."
  [{::keys [floor-plan sql-db run-query] :as env}]
  (let [{::floor-plan/keys [ident-keywords
                            batch-query
                            target-tables
                            target-columns
                            source-columns
                            join-statements
                            aggregators
                            cardinality]} floor-plan
        k                                 (env/dispatch-key env)]
    (if (contains? target-tables k)
      ;; this is an ident or a join, let's go for data
      (let [{:keys [query-string-input query-params join-children]}
            (process-query env)

            query-string
            (when (contains? ident-keywords k)
              (emitter/->query-string query-string-input))

            one?
            (= :one (get cardinality k))

            entities-ch
            (promise-chan)

            join-children-data-by-join-key-ch
            (promise-chan)

            entities-with-join-children-data-ch
            (promise-chan)]
        ;; debugging
        #_
        (println "dispatch key" k)
        (go (if query-string
              ;; for idents
              (>! entities-ch (<! (run-query sql-db (cons query-string query-params))))
              ;; joins don't have to build a query themselves
              ;; just look up the key in their parents data
              (let [parent (p/entity env)]
                (>! entities-ch (or (get parent (:ast env)) [])))))

        ;; debugging
        #_
        (go (let [entities (<! entities-ch)]
              (println "entities:" entities)))
        (go (let [entities         (<! entities-ch)
                  join-children-ch (to-chan join-children)]
              (if-not (and (seq entities) (seq join-children))
                (>! join-children-data-by-join-key-ch {})
                (loop [join-children-data-by-join-key {}]
                  (if-let [join-child (<! join-children-ch)]
                    (let [j             (:dispatch-key join-child)
                          ;; parent
                          source-column (get source-columns j)
                          ;; children
                          target-column (get target-columns j)

                          query-string-inputs
                          (for [e entities]
                            (process-query
                              (-> env
                                (assoc :ast join-child)
                                (as-> env
                                  (update env (get env ::p/entity-key)
                                    #(assoc (p/maybe-atom %) source-column (get e source-column)))))))

                          query-strings (map #(emitter/->query-string (:query-string-input %)) query-string-inputs)
                          all-params    (map :query-params query-string-inputs)

                          join-children-data
                          (<! (run-query sql-db (batch-query query-strings all-params)))]
                      (recur (assoc join-children-data-by-join-key
                               join-child (group-by target-column join-children-data))))
                    (>! join-children-data-by-join-key-ch join-children-data-by-join-key))))))
        ;; debugging
        #_
        (go (let [join-children-data-by-join-key (<! join-children-data-by-join-key-ch)]
              (println "join-children-data-by-join-key:" join-children-data-by-join-key )))
        (go (let [entities                       (<! entities-ch)
                  join-children-data-by-join-key (<! join-children-data-by-join-key-ch)]
              (put! entities-with-join-children-data-ch
                (for [e entities]
                  (let [child-joins
                        (into {}
                          (for [join-child join-children]
                            (let [j             (:dispatch-key join-child)
                                  source-column (get source-columns j)
                                  parent-id     (get e source-column)
                                  children      (get-in join-children-data-by-join-key
                                                  [join-child parent-id]
                                                  [])]
                              [join-child children])))]
                    (merge e child-joins))))))
        ;; debugging
        #_
        (go (let [entities-with-join-children-data (<! entities-with-join-children-data-ch)]
              (println "entities-with-join-children-data: " entities-with-join-children-data)))
        (let [result-chan (promise-chan)]
          (go (let [entities-with-join-children-data (<! entities-with-join-children-data-ch)]
                (if (seq entities-with-join-children-data)
                  (if (contains? aggregators k)
                    (>! result-chan (get (first entities-with-join-children-data) k))
                    (let [do-join (if one?
                                    #(p/join (first %2) %1)
                                    #(p/join-seq %1 %2))]
                      (>! result-chan (<! (do-join env entities-with-join-children-data)))))
                  (>! result-chan (if one? {} [])))))
          result-chan))

      ::p/continue)))
