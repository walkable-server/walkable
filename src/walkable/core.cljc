(ns walkable.core
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.ast :as ast]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.sql-query-builder.pathom-env :as env]
            [clojure.spec.alpha :as s]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.connect.planner :as pcp]))

(defn process-children*
  "Infers which columns to include in SQL query from child keys in env ast"
  [{:keys [ast] ::p/keys [placeholder-prefixes] :as env}]
  {:pre [(if placeholder-prefixes
           (set? placeholder-prefixes)
           true)]
   :post [#(s/valid? (s/keys :req-un [::join-children ::columns-to-query]) %)]}
  (let [{::floor-plan/keys [column-keywords required-columns source-columns]} (env/floor-plan env)

        all-children
        (ast/find-all-children ast
          {:placeholder? #(p/placeholder-key? env (:dispatch-key %))
           :leaf?        #(let [k (:dispatch-key %)]
                            (or ;; it's a column child
                              (contains? column-keywords k)
                              ;; it's a join child
                              (contains? source-columns k)
                              ;; it's a attr child that requires some other column children
                              (contains? required-columns k)))})

        {:keys [column-children join-children]}
        (->> all-children
          (group-by #(let [k (:dispatch-key %)]
                       (cond (contains? source-columns k)
                             :join-children

                             (contains? column-keywords k)
                             :column-children))))

        all-child-keys
        (into #{} (map :dispatch-key) all-children)

        child-column-keys
        (into #{} (map :dispatch-key) column-children)

        child-required-keys
        (->> all-child-keys (map #(get required-columns %)) (apply clojure.set/union))

        child-join-keys
        (map :dispatch-key join-children)

        ;; fixme: include ident columns here
        child-source-columns
        (into #{} (map #(get source-columns %)) child-join-keys)]
    {:join-children    (set join-children)
     :columns-to-query (clojure.set/union
                         child-column-keys
                         child-required-keys
                         child-source-columns)}))

(defn process-children
  "Infers which columns to include in SQL query from child keys in env ast"
  [env]
  (let [{::floor-plan/keys [aggregator-keywords]} (env/floor-plan env)
        k                                         (env/dispatch-key env)]
    (if (contains? aggregator-keywords k)
      {:columns-to-query #{k}
       :join-children    #{}}
      (process-children* env))))

(defn process-ident-children
  "Infers which columns to include in SQL query from child keys in env ast"
  [env]
  ;; fixme
  (println "processing ident children...")
  {:columns-to-query #{}
   :join-children    #{}})

(defn supplied-pagination
  "Processes :offset :limit and :order-by if provided in current
  EQL query params."
  [env]
  {:offset   (env/offset env)
   :limit    (env/limit env)
   :order-by (env/order-by env)})

(defn process-ident-condition
  [env]
  (when-let [compiled-ident-condition (env/compiled-ident-condition env)]
    (->> compiled-ident-condition
      (expressions/substitute-atomic-variables
        {:variable-values {`floor-plan/ident-value
                           (expressions/compile-to-string {} (env/ident-value env))}}))))

(defn process-pagination
  [env]
  {:pre  [(s/valid? (s/keys :req [::floor-plan/clojuric-names]) (env/floor-plan env))]
   :post [#(s/valid? (s/keys :req-un [::offset ::limit ::order-by ::order-by-columns]) %)]}
  (pagination/merge-pagination
    (env/pagination-default-fallbacks env)
    (env/pagination-fallbacks env)
    (supplied-pagination env)))

(defn process-supplied-condition
  [env]
  (let [{::floor-plan/keys [compiled-formulas join-filter-subqueries]}
        (env/floor-plan env)

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
  [env]
  (let [conditions
        (->> env
          ((juxt process-ident-condition
             process-supplied-condition
             env/compiled-extra-condition))
          (into [] (remove nil?)))]
    (when (seq conditions)
      (expressions/concatenate concat-with-and
        conditions))))

(defn child-join-process-conditions
  [env]
  (let [conditions
        (->> env
          ((juxt env/compiled-join-condition
             process-supplied-condition
             env/compiled-extra-condition))
          (into [] (remove nil?)))]
    (when (seq conditions)
      (expressions/concatenate concat-with-and
        conditions))))

(defn child-join-process-shared-conditions
  [env]
  (let [conditions
        (->> env
          ((juxt process-supplied-condition
             env/compiled-extra-condition))
          (into [] (remove nil?)))]
    (when (seq conditions)
      (expressions/concatenate concat-with-and
        conditions))))

(defn top-level-process-selection
  [env columns-to-query]
  (let [{::floor-plan/keys [compiled-selection]} (env/floor-plan env)
        compiled-normal-selection (mapv compiled-selection columns-to-query)]
    (expressions/concatenate  #(clojure.string/join ", " %)
      compiled-normal-selection)))

(def select-all {:raw-string "*" :params []})

(defn combine-params
  [& compiled-exprs]
  (into [] (comp (map :params) cat)
    compiled-exprs))

(defn ident-process-query
  [env]
  (let [{:keys [join-children columns-to-query]}
        (process-ident-children env)

        selection        (top-level-process-selection env columns-to-query)
        conditions       (top-level-process-conditions env)
        sql-query        {:raw-string
                          (emitter/->query-string
                            {:target-table   (env/target-table env)
                             :join-statement (env/join-statement env)
                             :selection      (:raw-string selection)
                             :conditions     (:raw-string conditions)})
                          :params (combine-params selection conditions)}]
    {:sql-query     sql-query
     :join-children join-children}))

(defn root-or-join-process-query
  [env]
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

(defn top-level-process-query*
  [env]
  (if (or (env/root-keyword env) (env/join-keyword env))
    (root-or-join-process-query env)
    (ident-process-query env)))

(defn child-join-process-shared-query*
  [env {:keys [order-by-columns]}]
  (let [{:keys [columns-to-query]} (process-children env)

        target-column    (env/target-column env)
        columns-to-query (-> (clojure.set/union columns-to-query order-by-columns)
                           (conj target-column))
        selection        (top-level-process-selection env columns-to-query)
        conditions       (child-join-process-shared-conditions env)
        having           (env/compiled-having env)
        sql-query        {:raw-string
                          (str "WITH walkable_common_join_children AS ("
                            (emitter/->query-string
                              {:target-table   (env/target-table env)
                               :join-statement (env/join-statement env)
                               :selection      (:raw-string selection)
                               :conditions     (:raw-string conditions)
                               :group-by       (env/compiled-group-by env)
                               :having         (:raw-string having)})
                            ")\n")
                          :params (combine-params selection conditions having)}]
    sql-query))

(defn child-join-process-shared-aggregator-query*
  [env]
  (let [target-column    (env/target-column env)
        columns-to-query #{target-column}
        selection        (top-level-process-selection env columns-to-query)
        conditions       (child-join-process-shared-conditions env)
        having           (env/compiled-having env)
        sql-query        {:raw-string
                          (str "WITH walkable_common_join_children AS ("
                            (emitter/->query-string
                              {:target-table   (env/target-table env)
                               :join-statement (env/join-statement env)
                               :selection      (:raw-string selection)
                               :conditions     (:raw-string conditions)
                               :group-by       (env/compiled-group-by env)
                               :having         (:raw-string having)})
                            ")\n")
                          :params (combine-params selection conditions having)}]
    sql-query))

(defn child-join-process-individual-aggregator-query*
  [env]
  (let [selection  (env/compiled-aggregator-selection env)
        conditions (child-join-process-conditions env)
        sql-query {:raw-string
                   (emitter/->query-string
                     {:target-table   (env/target-table env)
                      :join-statement (env/join-statement env)
                      :selection      (:raw-string selection)
                      :conditions     (:raw-string conditions)})
                   :params (combine-params selection conditions)}]
    sql-query))

(defn child-join-process-individual-query*
  [env {:keys [offset limit order-by order-by-columns]}]
  (let [{:keys [columns-to-query]} (process-children env)
        target-column              (env/target-column env)

        columns-to-query
        (-> (clojure.set/union columns-to-query order-by-columns)
          (conj target-column))

        selection
        (top-level-process-selection env columns-to-query)

        conditions (child-join-process-conditions env)

        having    (env/compiled-having env)
        sql-query {:raw-string
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
    sql-query))

(defn child-join-process-individual-aggregator-query-cte*
  [env]
  (let [selection  (env/compiled-aggregator-selection env)
        conditions (env/compiled-join-condition-cte env)

        sql-query {:raw-string
                   (emitter/->query-string
                     {:target-table "walkable_common_join_children"
                      :selection    (:raw-string selection)
                      :conditions   (:raw-string conditions)})
                   :params (combine-params selection conditions)}]
    sql-query))

(defn child-join-process-individual-query-cte*
  [env {:keys [offset limit order-by]}]
  (let [selection  select-all
        conditions (env/compiled-join-condition-cte env)

        sql-query {:raw-string
                   (emitter/->query-string
                     {:target-table "walkable_common_join_children"

                      :selection  (:raw-string selection)
                      :conditions (:raw-string conditions)

                      :offset   offset
                      :limit    limit
                      :order-by order-by})
                   :params (combine-params selection conditions)}]
    sql-query))

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
  [env {:keys [computed-graphs variables]}]
  (let [getters (select-keys (env/compiled-variable-getters env) variables)]
    (into {}
      (map (fn [[k f]]
             (let [v (f env computed-graphs)]
               ;; wrap in single-raw-string to feed
               ;; `expressions/substitute-atomic-variables`
               [k (expressions/single-raw-string v)])))
      getters)))

(defn process-variables
  [env variables]
  (compute-variables env
    {:computed-graphs (compute-graphs env variables)
     :variables       variables}))

(defn top-level-process-query
  [env]
  (let [query           (top-level-process-query* env)
        sql-query       (:sql-query query)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (assoc query :sql-query
      (expressions/substitute-atomic-variables
        {:variable-values variable-values} sql-query))))

(defn child-join-process-shared-query
  [env pagination]
  (let [sql-query       (child-join-process-shared-query* env pagination)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (expressions/substitute-atomic-variables
      {:variable-values variable-values} sql-query)))

(defn child-join-process-shared-aggregator-query
  [env]
  (let [sql-query       (child-join-process-shared-aggregator-query* env)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (expressions/substitute-atomic-variables
      {:variable-values variable-values} sql-query)))

(defn child-join-process-individual-query
  [env pagination]
  (let [sql-query       (child-join-process-individual-query* env pagination)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (expressions/substitute-atomic-variables
      {:variable-values variable-values} sql-query)))

(defn child-join-process-individual-aggregator-query
  [env]
  (let [sql-query       (child-join-process-individual-aggregator-query* env)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (expressions/substitute-atomic-variables
      {:variable-values variable-values} sql-query)))

(defn child-join-process-individual-query-cte
  [env pagination]
  (let [sql-query       (child-join-process-individual-query-cte* env pagination)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (expressions/substitute-atomic-variables
      {:variable-values variable-values} sql-query)))

(defn child-join-process-individual-aggregator-query-cte
  [env]
  (let [sql-query       (child-join-process-individual-aggregator-query-cte* env)
        variable-values (process-variables env
                          (expressions/find-variables sql-query))]
    (expressions/substitute-atomic-variables
      {:variable-values variable-values} sql-query)))

(defn build-parameterized-sql-query
  [{:keys [raw-string params]}]
  (vec (cons raw-string params)))

(defn top-level
  [env]
  (let [{::keys [db query]} (env/config env)
        {:keys [sql-query join-children]} (top-level-process-query env)]
    {:join-children join-children
     :entities      (cond
                      ;; for roots
                      (env/root-keyword env)
                      (query db (build-parameterized-sql-query sql-query))

                      ;; joins don't have to build a query themselves
                      ;; just look up the key in cached data
                      ;; fetched by their parents
                      (env/join-keyword env)
                      (let [p (env/parent-path env)
                            scv (env/source-column-value env)
                            parent-data (p/cached env p ::not-cached)
                            r (get-in parent-data [(env/dispatch-key env) scv])]
                        (if (= parent-data ::not-cached)
                          []
                          r))
                      ;; fixme: idents get empty data for now
                      (env/ident-keyword env)
                      [{}]

                      :else
                      [{}])}))

(defn source-column-variable-values
  [v]
  {:variable-values {`floor-plan/source-column-value
                     (expressions/compile-to-string {} v)}})

(defn process-join-children
  [child-env aggregator?]
  (if aggregator?
    {:unbound-individual-query
     (child-join-process-individual-aggregator-query child-env)}
    (let [pagination (process-pagination child-env)]
      {:unbound-individual-query
       (child-join-process-individual-query child-env pagination)})))

(defn process-join-children-cte
  [child-env aggregator?]
  (if aggregator?
    {:shared-query
     (child-join-process-shared-aggregator-query child-env)
     :unbound-individual-query
     (child-join-process-individual-aggregator-query-cte child-env)}
    (let [pagination (process-pagination child-env)]
      {:shared-query
       (child-join-process-shared-query child-env pagination)
       :unbound-individual-query
       (child-join-process-individual-query-cte child-env pagination)})))

(defn final-query
  [{:keys [batch-query cet? aggregator?
           child-env source-column entities]}]
  (let [{:keys [shared-query unbound-individual-query]}
        (if cet?
          (process-join-children-cte child-env aggregator?)
          (process-join-children child-env aggregator?))

        individual-queries
        (for [e    entities
              :let [v (get e source-column)]
              :when (not (nil? v))]
          (->> unbound-individual-query
            (expressions/substitute-atomic-variables
              (source-column-variable-values v))))

        batched-individuals (batch-query individual-queries)]
    (if cet?
      (expressions/concatenate #(apply str %)
        [shared-query batched-individuals])
      batched-individuals)))

(defn join-children-data
  [env {:keys [entities join-children]}]
  (let [{::floor-plan/keys [batch-query
                            aggregator-keywords
                            cte-keywords
                            target-columns
                            source-columns]}
        (env/floor-plan env)]
    (when (and (seq entities) (seq join-children))
      (let [f (fn [join-child-ast]
                (let [j (:dispatch-key join-child-ast)

                      aggregator? (contains? aggregator-keywords j)
                      cet?        (contains? cte-keywords j)

                      ;; parent
                      source-column (get source-columns j)
                      ;; children
                      target-column (get target-columns j)

                      child-env (assoc env :ast join-child-ast)

                      query
                      (final-query {:batch-query batch-query
                                    :cet? cet?
                                    :aggregator? aggregator?
                                    :child-env child-env
                                    :source-column source-column
                                    :entities entities})]
                  [j
                   {:target-column target-column
                    :query         (build-parameterized-sql-query query)}]))]
        (mapv f join-children)))))

(defn join-children-data-by-join-key
  [env {:keys [entities join-children]}]
  (let [{::keys [db query]} (env/config env)
        f (fn [[join-child {:keys [target-column] q :query}]]
            (let [data (query db q)]
              [join-child (group-by target-column data)]))]
    (into {}
      (map f)
      (join-children-data env {:entities entities :join-children join-children}))))

(defn dynamic-resolver
  [env]
  ;; this is a root or a join, let's go for data
  (if (or (env/root-keyword env) (env/join-keyword env) (env/ident-keyword env))
    (let [{:keys [join-children entities]} (top-level env)]
      (when (seq join-children)
        (p/cached env (::p/path env)
          (join-children-data-by-join-key env
            {:entities      entities
             :join-children join-children})))
      (if-let [k (or (env/root-keyword env) (env/join-keyword env))]
        {k ((env/return env) entities)}
        (first entities)))))

(defn compute-indexes [resolver-sym ios]
  (reduce (fn [acc x] (pc/add acc resolver-sym x))
    {}
    ios))

(defn connect-plugin
  [{:keys [resolver-sym db query floor-plan
           inputs-outputs autocomplete-ignore
           resolver]
    :or   {resolver     dynamic-resolver
           resolver-sym `walkable-resolver}}]
  (let [provided-indexes (compute-indexes resolver-sym inputs-outputs)
        config {::db           db
                ::query        query
                ::resolver-sym resolver-sym
                ::floor-plan   (floor-plan/compile-floor-plan
                                 (assoc floor-plan :idents (::pc/idents provided-indexes)))}]
    {::p/intercept-output (fn [_env v] v)
     ::p/wrap-parser2
     (fn [parser {::p/keys [plugins]}]
       (let [resolve-fn (fn [env _] (resolver env))
             all-indexes (merge provided-indexes
                           {::pc/index-resolvers
                            {resolver-sym
                             {::config               config
                              ::pc/sym               resolver-sym
                              ::pc/cache?            false
                              ::pc/dynamic-resolver? true
                              ::pc/output            []
                              ::pc/resolve           resolve-fn}}
                            ::pc/autocomplete-ignore (or autocomplete-ignore #{})})
             idx-atoms (keep ::pc/indexes plugins)]
         (doseq [idx* idx-atoms]
           (swap! idx* pc/merge-indexes all-indexes))
         (fn [env tx] (parser env tx))))}))
