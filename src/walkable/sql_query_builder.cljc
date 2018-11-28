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

(defn process-children
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

(defn process-pagination [{::keys [floor-plan] :as env}]
  {:pre  [(s/valid? (s/keys :req [::floor-plan/clojuric-names]) floor-plan)]
   :post [#(s/valid? (s/keys :req-un [::offset ::limit ::order-by ::order-by-columns]) %)]}
  (pagination/process-pagination
    (::floor-plan/clojuric-names floor-plan)
    (supplied-pagination env)
    (env/pagination-fallbacks env)))

(defn ident->condition
  "Converts given ident key in env to equivalent condition dsl."
  [env key]
  {:pre  [(s/valid? ::expressions/namespaced-keyword key)
          (s/valid? (s/keys :req-un [::ast]) env)]
   :post [#(s/valid? ::expressions/expression %)]}
  (conj [:= key] (env/ident-value env)))

(defn process-conditions
  "Combines all conditions to produce the final WHERE
  statement. Returns a vector (which implies an AND) of:

  - ident-condition: eg [:person/by-id 1] will result `WHERE person.id
  = 1`

  - join-condition: to filter the joined table given the attribute of
  the entity from upper level.

  - extra-condition: extra constraints for an ident or a join defined
  in floor-plan.

  - supplied-condition: ad-hoc condition supplied in om.next
  query (often by client apps)"
  [{::keys [floor-plan] :as env}]
  (let [{::floor-plan/keys [ident-conditions]} floor-plan

        e (p/entity env)
        k (env/dispatch-key env)

        ident-condition
        (when-let [condition (get ident-conditions k)]
          (ident->condition env condition))

        target-column (env/target-column env)

        source-column (env/source-column env)

        join-condition
        (when target-column ;; if it's a join
          [:= target-column (get e source-column)])

        extra-condition (env/extra-condition env)

        supplied-condition
        (get-in env [:ast :params :filters])

        supplied-condition
        (when (s/valid? ::expressions/expression supplied-condition)
          supplied-condition)]
    [ident-condition join-condition extra-condition supplied-condition]))

(defn parameterize-all-conditions
  [{::keys [floor-plan] :as env} columns-to-query]
  (let [{::floor-plan/keys [column-names join-filter-subqueries]} floor-plan

        all-conditions (clean-up-all-conditions (process-conditions env))]
    (when all-conditions
      (->> all-conditions
        (expressions/parameterize {:pathom-env             env
                                   :column-names           column-names
                                   :join-filter-subqueries join-filter-subqueries})
        ((juxt :raw-string :params))))))

(defn process-selection
  [{::keys [floor-plan] :as env} columns-to-query]
  (let [{::floor-plan/keys [column-names clojuric-names]} floor-plan

        target-column (env/target-column env)]
    (concat
      (mapv (fn [k]
              (let [column-name   (get column-names k)
                    clojuric-name (get clojuric-names k)]
                (if (string? column-name)
                  {:raw-string (str column-name " AS " clojuric-name)
                   :params     []}
                  ;; not string? it must be a pseudo-column
                  (let [form (s/conform ::expressions/expression (if (fn? column-name)
                                                                   (column-name env)
                                                                   column-name))]
                    (when-not (= ::s/invalid form)
                      (expressions/inline-params {}
                        {:raw-string (str "(?) AS " clojuric-name)
                         :params     [(expressions/process-expression
                                        {:column-names column-names}
                                        form)]}))))))
        (if target-column
          (remove #(= % target-column) columns-to-query)
          columns-to-query))
      (when target-column
        (let [form (s/conform ::expressions/expression (env/source-column-value env))]
          (when-not (= ::s/invalid form)
            [(expressions/inline-params {}
               {:raw-string (str "? AS " (get clojuric-names target-column))
                :params     [(expressions/process-expression
                               {:column-names column-names}
                               form)]})]))))))

(defn parameterize-all-selection
  [env columns-to-query]
  (let [column-names (-> env ::floor-plan ::floor-plan/column-names)
        xs (process-selection env columns-to-query)]
    (->> {:raw-string (->> (repeat (count xs) \?)
                        (clojure.string/join ", "))
          :params     xs}
      (expressions/inline-params {})
      ((juxt :raw-string :params)))))

(defn process-all-params
  "Replaces any keyword found in all-params with their corresponding
  column-name"
  [env all-params]
  (let [column-names (-> env ::floor-plan ::floor-plan/column-names)]
    (mapv (fn stringify-keywords [param]
            (if (expressions/namespaced-keyword? param)
              (get column-names param)
              param))
      all-params)))

(defn process-query
  "Helper function for pull-entities. Outputs

  - query-string-input and query-params: to build SQL query to fetch
  data for entities of current level (using ->query-string)

  - join-children: set of direct nested levels that will require their
  own SQL query."
  [{::keys [floor-plan] :as env}]
  {:pre [(s/valid? (s/keys :req [::floor-plan/column-names
                                 ::floor-plan/clojuric-names]
                     :opt [::floor-plan/join-statements
                           ::floor-plan/target-tables
                           ::floor-plan/target-columns])
           floor-plan)]}
  (let [{::floor-plan/keys [aggregators]}        floor-plan
        k                                        (env/dispatch-key env)
        {:keys [join-children columns-to-query]} (if (contains? aggregators k)
                                                   {:columns-to-query #{k}
                                                    :join-children    #{}}
                                                   (process-children env))

        {:keys [offset limit order-by order-by-columns]} (process-pagination env)

        columns-to-query                (clojure.set/union columns-to-query order-by-columns)
        [selection select-params]       (parameterize-all-selection env columns-to-query)
        [where-conditions where-params] (parameterize-all-conditions env columns-to-query)]
    {:query-string-input {:target-table     (env/target-table env)
                          :join-statement   (env/join-statement env)
                          :selection        selection
                          :where-conditions where-conditions
                          :offset           offset
                          :limit            limit
                          :order-by         order-by}
     :query-params       (process-all-params env (concat select-params where-params))
     :join-children      join-children}))

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

            entities
            (if query-string
              ;; for idents
              (run-query sql-db
                (if query-params
                  (cons query-string query-params)
                  query-string))
              ;; joins don't have to build a query themselves
              ;; just look up the key in their parents data
              (let [parent (p/entity env)]
                (get parent (:ast env))))

            ;;join-child-queries
            join-children-data-by-join-key
            (when (and (seq entities) (seq join-children))
              (into {}
                (for [join-child join-children]
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
                        (run-query sql-db (batch-query query-strings all-params))]
                    [join-child (group-by target-column join-children-data)]))))

            entities-with-join-children-data
            (for [e entities]
              (let [child-joins
                    (into {}
                      (for [join-child join-children]
                        (let [j             (:dispatch-key join-child)
                              source-column (get source-columns j)
                              parent-id     (get e source-column)
                              children      (get-in join-children-data-by-join-key
                                              [join-child parent-id])]
                          [join-child children])))]
                (merge e child-joins)))

            one?
            (= :one (get cardinality k))

            do-join
            (if (contains? aggregators k)
              #(get (first %2) k)
              (if one?
                #(p/join (first %2) %1)
                #(p/join-seq %1 %2)))]
        (if (seq entities-with-join-children-data)
          (do-join env entities-with-join-children-data)
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
