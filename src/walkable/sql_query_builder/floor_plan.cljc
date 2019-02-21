(ns walkable.sql-query-builder.floor-plan
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.pathom-env :as env]
            [com.wsscode.pathom.core :as p]
            [clojure.spec.alpha :as s]
            [plumbing.graph :as graph]
            [clojure.core.async :as async
             :refer [go go-loop <! >! put! promise-chan to-chan]]))

(defn column-names
  "Makes a hash-map of keywords and their equivalent column names"
  [emitter ks]
  (zipmap ks
    (map #(emitter/column-name emitter %) ks)))

(defn clojuric-names
  "Makes a hash-map of keywords and their Clojuric name (to be use as
  sql's SELECT aliases"
  [emitter ks]
  (zipmap ks
    (map #(emitter/clojuric-name emitter %) ks)))

(s/def ::without-join-table
  (s/coll-of ::expressions/namespaced-keyword
    :count 2))

(s/def ::with-join-table
  (s/coll-of ::expressions/namespaced-keyword
    :count 4))

(s/def ::join-seq
  (s/or
   :without-join-table ::without-join-table
   :with-join-table    ::with-join-table))

(defn join-statements
  "Helper for compile-floor-plan. Generates JOIN statement strings for all
  join keys given their join sequence."
  [emitter join-seq]
  {:pre  [(s/valid? ::join-seq join-seq)]
   :post [string?]}
  (let [[tag] (s/conform ::join-seq join-seq)]
    (when (= :with-join-table tag)
      (let [[source join-source join-target target] join-seq]
        (str
          " JOIN " (emitter/table-name emitter target)
          " ON " (emitter/column-name emitter join-target)
          " = " (emitter/column-name emitter target))))))

(s/def ::join-specs
  (s/coll-of (s/tuple ::expressions/namespaced-keyword ::join-seq)))

(defn source-column
  [join-seq]
  (first join-seq))

(defn target-column
  [join-seq]
  (second join-seq))

(defn target-table
  [emitter join-seq]
  (emitter/table-name emitter (target-column join-seq)))

(defn join-filter-subquery
  [emitter joins]
  (str
    (emitter/column-name emitter (source-column joins))
    " IN ("
    (emitter/->query-string
      {:selection      (emitter/column-name emitter (target-column joins))
       :target-table   (target-table emitter joins)
       :join-statement (join-statements emitter joins)})
    " WHERE ?)"))

(defn joins->target-tables
  "Produces map of join keys to their corresponding source table name."
  [emitter joins]
  {:pre  [(s/valid? ::join-specs joins)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (target-table emitter join-seq)))
    {} joins))

(defn joins->target-columns
  "Produces map of join keys to their corresponding target column."
  [joins]
  {:pre  [(s/valid? ::join-specs joins)]
   :post [#(s/valid? ::keyword-keyword-map %)]}
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (target-column join-seq)))
    {} joins))

(defn joins->source-columns
  "Produces map of join keys to their corresponding source column."
  [joins]
  {:pre  [(s/valid? ::join-specs joins)]
   :post [#(s/valid? ::keyword-keyword-map %)]}
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (source-column join-seq)))
    {} joins))

(s/def ::conditional-ident
  (s/tuple keyword? ::expressions/namespaced-keyword))

(s/def ::unconditional-ident
  (s/tuple keyword? string?))

(defn conditional-idents->target-tables
  "Produces map of ident keys to their corresponding source table name."
  [emitter idents]
  {:pre  [(s/valid? (s/coll-of ::conditional-ident) idents)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (reduce (fn [result [ident-key column-keyword]]
            (assoc result ident-key
              (emitter/table-name emitter column-keyword)))
          {} idents))

(defn unconditional-idents->target-tables
  "Produces map of ident keys to their corresponding source table name."
  [emitter idents]
  {:pre  [(s/valid? (s/coll-of ::unconditional-ident) idents)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (reduce (fn [result [ident-key raw-table-name]]
            (assoc result ident-key
              (emitter/table-name* emitter raw-table-name)))
    {} idents))

(s/def ::multi-keys
  (s/coll-of (s/tuple (s/or :single-key keyword?
                        :multiple-keys (s/coll-of keyword))
               (constantly true))))

(s/def ::single-keys
  (s/coll-of (s/tuple keyword? (constantly true))))

(defn expand-multi-keys
  "Expands a map where keys can be a vector of keywords to pairs where
  each keyword has its own entry."
  [m]
  {:pre  [(s/valid? ::multi-keys m)]
   :post [#(s/valid? ::single-keys %)]}
  (reduce (fn [result [ks v]]
            (if (sequential? ks)
              (let [new-pairs (mapv (fn [k] [k v]) ks)]
                (vec (concat result new-pairs)))
              (conj result [ks v])))
    [] m))

(defn flatten-multi-keys
  "Expands multiple keys then group values of the same key"
  [m]
  {:pre  [(s/valid? ::multi-keys m)]
   :post [#(s/valid? ::single-keys %)]}
  (let [expanded  (expand-multi-keys m)
        key-set   (set (map first expanded))
        keys+vals (mapv (fn [current-key]
                          (let [current-vals (mapv second (filter (fn [[k v]] (= current-key k)) expanded))]
                            [current-key (if (= 1 (count current-vals))
                                           (first current-vals)
                                           current-vals)]))
                    key-set)]
    (into {} keys+vals)))

(defn compile-join-statements
  [emitter joins]
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (join-statements emitter join-seq)))
    {} joins))

(defn compile-join-filter-subqueries
  [emitter joins]
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (join-filter-subquery emitter join-seq)))
    {} joins))

(defn expand-reversed-joins [joins reversed-joins]
  (let [more (reduce (fn [result [backward forward]]
                       (assoc result backward
                         (reverse (get joins forward))))
               {} reversed-joins)]
    ;; if a join exists in joins, keep it instead of
    ;; reversing associated backward join
    (merge more joins)))

(defn expand-denpendencies* [m]
  (reduce (fn [result [k vs]]
            (assoc result k
              (set (flatten (mapv #(if-let [deps (get m %)]
                                     (vec deps)
                                     %)
                              vs)))))
    {} m))

(defn expand-denpendencies [m]
  (let [m' (expand-denpendencies* m)]
    (if (= m m')
      m
      (expand-denpendencies m'))))

(defn separate-idents*
  "Helper function for compile-floor-plan. Separates all user-supplied
  idents to unconditional idents and conditional idents for further
  processing."
  [idents]
  (reduce (fn [result [k v]]
            (if (string? v)
              (assoc-in result [:unconditional-idents k] v)
              (assoc-in result [:conditional-idents k] v)))
    {:unconditional-idents {}
     :conditional-idents   {}}
    idents))

(defn separate-idents
  [{:keys [joins idents] :as floor-plan}]
  (-> floor-plan
    (merge ;; will add `:unconditional-idents` and `:conditional-idents`:
      (separate-idents* idents)
      {:target-columns (joins->target-columns joins)
       :source-columns (joins->source-columns joins)})))

(defn unbound-expression?
  [compiled-expression]
  (boolean (some expressions/atomic-variable? (:params compiled-expression))))

(defn rotate [coll]
  (take (count coll) (drop 1 (cycle coll))))

(defn compile-formulas-once [compiled-true-columns formulas]
  (reduce-kv (fn [result k expression]
               (let [compiled (expressions/compile-to-string {} expression)]
                 (if (unbound-expression? compiled)
                   (update result :unbound assoc k compiled)
                   (update result :bound assoc k compiled))))
    {:unbound {}
     :bound   compiled-true-columns}
    formulas))

(defn compile-formulas-recursively [{:keys [unbound bound]}]
  (loop [limit   100
         unbound (seq unbound)
         bound   bound]
    (if-let [item (and (pos? limit) (first unbound))]
      (let [[k compiled-expression] item

            attempt  (expressions/substitute-atomic-variables
                       {:variable-values bound}
                       compiled-expression)
            new-item [k attempt]]
        (if (unbound-expression? attempt)
          (recur (dec limit)
            (rotate (cons new-item (rest unbound)))
            bound)
          (recur (dec limit)
            (rest unbound)
            (assoc bound k attempt))))
      ;; no more work. Exiting loop...
      {:unbound (into {} unbound)
       :bound   bound})))

(defn column-dependencies
  [{:keys [bound unbound]}]
  (let [all-columns    (set (concat (keys bound) (keys unbound)))]
    (reduce-kv (fn [acc k compiled-expression]
                 (let [p                  (-> compiled-expression :params)
                       all-vars           (set (map :name (filter expressions/atomic-variable? p)))
                       column-vars        (set (filter keyword? all-vars))
                       undeclared-columns (clojure.set/difference column-vars all-columns)]
                   (assert (not (contains? column-vars k))
                     (str "Circular dependency: " k " depends on itself"))
                   (assert (empty? undeclared-columns)
                     (str "Missing definition for columns: " undeclared-columns
                       ". You may want to add them to :columns or :pseudo-columns."))
                   (assoc acc k column-vars)))
      {}
      unbound)))

(defn compile-selection [compiled-formula clojuric-name]
  (expressions/inline-params {}
    {:raw-string "(?) AS ?"
     :params     [compiled-formula
                  (expressions/verbatim-raw-string clojuric-name)]}))


(defn compile-getter
  [{function :fn k :key :keys [cached?]}]
  (let [f (if cached?
            (fn haha [env _computed-graphs]
              (p/cached env [:walkable/variable-getter k]
                (function env)))
            (fn hihi [env _computed-graphs]
              (function env)))]
    [k f]))

(defn compile-variable-getters
  [{:keys [variable-getters] :as env}]
  (let [getters
        (into {} (map compile-getter) variable-getters)]
    (-> env
      (assoc :compiled-variable-getters getters)
      (dissoc :variable-getters))))

(defn member->graph-id
  [graphs]
  (->> graphs
    (map-indexed
      (fn [index {:keys [graph namespace]}]
        (let [xs (keys graph)]
          (mapv #(do [(symbol namespace (name %)) index]) xs))))
    (apply concat)
    (into {})))

(defn compile-graph
  [index {:keys [graph cached? lazy?]}]
  (let [compiled-graph
        #?(:clj
           (if lazy?
             (graph/lazy-compile graph)
             (graph/compile graph))
           :cljs
           (graph/compile graph))
        function #(compiled-graph {:env %})]
    [index
     (if cached?
       (fn [env]
         (p/cached env [:walkable/variable-getter-graph index]
           (function env)))
       function)]))

(defn compile-graph-member-getter
  [result variable graph-index]
  (let [k (keyword (name variable))]
    (assoc result variable
      (fn [_env computed-graphs]
        (get-in computed-graphs [graph-index k])))))

(defn compile-variable-getter-graphs
  [{:keys [variable-getter-graphs] :as env}]
  (let [compiled-graphs
        (->> variable-getter-graphs
          (into {} (map-indexed compile-graph)))

        variable->graph-id
        (member->graph-id variable-getter-graphs)

        getters
        (reduce-kv
          compile-graph-member-getter
          {}
          variable->graph-id)]
    (-> env
      (assoc :compiled-variable-getter-graphs compiled-graphs)
      (assoc :variable->graph-index variable->graph-id)
      (update :compiled-variable-getters merge getters)
      (dissoc :variable-getter-graphs))))

(defn compile-true-columns
  "Makes a hash-map of keywords and their equivalent compiled form."
  [emitter ks]
  (zipmap ks
    (map #(expressions/verbatim-raw-string (emitter/column-name emitter %)) ks)))

(defn compile-formulas
  [{:keys [true-columns pseudo-columns aggregators emitter] :as floor-plan}]
  (let [compiled-formulas       (-> (compile-true-columns emitter true-columns)
                                  (compile-formulas-once (merge pseudo-columns aggregators))
                                  compile-formulas-recursively)
        _ok?                    (column-dependencies compiled-formulas)
        {:keys [bound unbound]} compiled-formulas
        compiled-formulas       (merge bound unbound)]
    (-> (dissoc floor-plan :true-columns :pseudo-columns :aggregators)
      (assoc :compiled-formulas compiled-formulas))))

(defn compile-formulas-with-aliases
  [{:keys [compiled-formulas clojuric-names] :as floor-plan}]
  (let [compiled-selection
        (reduce-kv (fn [acc k f]
                     (assoc acc k (compile-selection f (get clojuric-names k))))
          {}
          compiled-formulas)]
    (assoc floor-plan :compiled-selection compiled-selection)))

(defn compile-ident-conditions
  [{:keys [conditional-idents compiled-formulas] :as floor-plan}]
  (let [compiled-ident-conditions
        (reduce-kv (fn [acc k ident-key]
                     (assoc acc k
                       (expressions/substitute-atomic-variables
                         {:variable-values compiled-formulas}
                         (expressions/compile-to-string {}
                           [:= ident-key (expressions/av `ident-value)]))))
          {}
          conditional-idents)]
    (-> floor-plan
      (assoc :compiled-ident-conditions compiled-ident-conditions)
      (dissoc :conditional-idents :unconditional-idents))))

(defn compile-join-selection
  [{:keys [joins clojuric-names target-columns] :as floor-plan}]
  (let [compiled-join-selection
        (reduce-kv (fn [acc k join-seq]
                     (let [target-column (get target-columns k)]
                       (assoc acc k
                         (compile-selection
                           {:raw-string "?"
                            :params [(expressions/av `source-column-value)]}
                           (get clojuric-names target-column)))))
          {}
          joins)]
    (-> floor-plan
      (assoc :compiled-join-selection compiled-join-selection))))

(defn compile-aggregator-selection
  [{:keys [aggregator-keywords compiled-selection clojuric-names target-columns] :as floor-plan}]
  (let [compiled-aggregator-selection
        (reduce (fn [acc k]
                  (let [target-column
                        (get target-columns k)

                        aggregator-selection
                        (get compiled-selection k)

                        source-column-selection
                        (compile-selection
                          {:raw-string "?"
                           :params     [(expressions/av `source-column-value)]}
                          (get clojuric-names target-column))]
                    (assoc acc k
                      (expressions/concatenate #(clojure.string/join ", " %)
                        [source-column-selection aggregator-selection]))))
          {}
          aggregator-keywords)]
    (-> floor-plan
      (assoc :compiled-aggregator-selection compiled-aggregator-selection))))

(defn compile-join-conditions
  [{:keys [joins compiled-formulas target-columns clojuric-names] :as floor-plan}]
  (let [compiled-join-conditions
        (reduce-kv (fn [acc k join-seq]
                     (let [target-column (get target-columns k)
                           clojuric-name (get clojuric-names target-column)]
                       (assoc acc k
                         (expressions/substitute-atomic-variables
                           {:variable-values compiled-formulas}
                           {:raw-string (str clojuric-name "= ?")
                            :params     [(expressions/av `source-column-value)]}))))
          {}
          joins)]
    (-> floor-plan
      (assoc :compiled-join-conditions compiled-join-conditions)
      (dissoc :joins))))

(defn compile-extra-conditions
  [{:keys [extra-conditions compiled-formulas join-filter-subqueries] :as floor-plan}]
  (let [compiled-extra-conditions
        (reduce-kv (fn [acc k extra-condition]
                     (assoc acc k
                       (->> extra-condition
                         (expressions/compile-to-string
                           {:join-filter-subqueries join-filter-subqueries})
                         (expressions/substitute-atomic-variables
                           {:variable-values compiled-formulas}))))
          {}
          extra-conditions)]
    (-> floor-plan
      (assoc :compiled-extra-conditions compiled-extra-conditions)
      (dissoc :extra-conditions))))

(defn prefix-having [compiled-having]
  (expressions/inline-params {}
    {:raw-string " HAVING (?)"
     :params     [compiled-having]}))

(defn compile-having
  [{:keys [compiled-formulas join-filter-subqueries]} having-condition]
  (->> having-condition
    (expressions/compile-to-string
      {:join-filter-subqueries join-filter-subqueries})
    (expressions/substitute-atomic-variables
      {:variable-values compiled-formulas})
    prefix-having))

(defn compile-group-by
  [compiled-formulas group-by-keys]
  (->> group-by-keys
    (map compiled-formulas)
    (map :raw-string)
    (clojure.string/join ", ")
    (str " GROUP BY ")))

(defn compile-grouping
  [{:keys [grouping compiled-formulas] :as floor-plan}]
  (let [compiled-grouping
        (reduce-kv (fn [acc k {group-by-keys :group-by having-condition :having}]
                     (let [compiled-group-by
                           (compile-group-by compiled-formulas group-by-keys)
                           compiled-having
                           (when having-condition
                             (compile-having floor-plan having-condition))]
                       (-> acc
                         (assoc-in [:compiled-group-by k] compiled-group-by)
                         (assoc-in [:compiled-having k] compiled-having))))
          {:compiled-group-by {}
           :compiled-having   {}}
          grouping)]
    (-> floor-plan
      (merge compiled-grouping)
      (dissoc :grouping))))

(defn compile-pagination-fallbacks
  [{:keys [emitter clojuric-names pagination-fallbacks] :as floor-plan}]
  (let [compiled-pagination-fallbacks
        (pagination/compile-fallbacks emitter clojuric-names pagination-fallbacks)]
    (-> floor-plan
      (assoc :compiled-pagination-fallbacks compiled-pagination-fallbacks)
      (dissoc :pagination-fallbacks))))

(defn join-one [env entities]
  (p/join (first entities) env))

(defn compile-return-or-join
  [{:keys [target-tables aggregator-keywords cardinality] :as floor-plan}]
  (let [compiled-return-or-join
        (reduce (fn [acc k]
                  (let [aggregator? (contains? aggregator-keywords k)
                        one?        (= :one (get cardinality k))
                        f           (if aggregator?
                                      #(get (first %2) k)
                                      (if one?
                                        join-one
                                        p/join-seq))]
                    (assoc acc k f)))
          {}
          (keys target-tables))]
    (assoc floor-plan :return-or-join compiled-return-or-join)))

(defn compile-return-or-join-async
  [{:keys [target-tables aggregator-keywords cardinality] :as floor-plan}]
  (let [compiled-return-or-join-async
        (reduce (fn [acc k]
                  (let [aggregator? (contains? aggregator-keywords k)
                        one?        (= :one (get cardinality k))
                        f           (if aggregator?
                                      #(go (get (first %2) k))
                                      (if one?
                                        join-one
                                        p/join-seq))]
                    (assoc acc k f)))
          {}
          (keys target-tables))]
    (assoc floor-plan :return-or-join-async compiled-return-or-join-async)))

(def floor-plan-keys
  [:aggregator-keywords
   :batch-query
   :cardinality
   :clojuric-names
   :column-keywords
   :compiled-extra-conditions
   :compiled-formulas
   :compiled-group-by
   :compiled-having
   :compiled-ident-conditions
   :compiled-join-conditions
   :compiled-join-selection
   :compiled-selection
   :emitter
   :ident-keywords
   :join-filter-subqueries
   :join-keywords
   :join-statements
   :compiled-pagination-fallbacks
   :required-columns
   :reversed-joins
   :return-or-join
   :return-or-join-async
   :compiled-variable-getters
   :compiled-variable-getter-graphs
   :variable->graph-index
   :source-columns
   :target-columns
   :target-tables])

(defn kmap [ks]
  (let [this-ns (namespace ::foo)]
    (zipmap ks (map #(keyword this-ns (name %)) ks))))

(defn internalize-keywords
  [floor-plan]
  (-> floor-plan
    (select-keys floor-plan-keys)
    (clojure.set/rename-keys (kmap floor-plan-keys))))

(def compile-floor-plan*
  (comp compile-grouping
    compile-pagination-fallbacks
    compile-variable-getter-graphs
    compile-variable-getters
    compile-return-or-join-async
    compile-return-or-join
    compile-extra-conditions
    compile-join-conditions
    compile-join-selection
    compile-ident-conditions
    compile-formulas-with-aliases
    compile-formulas))

(defn columns-in-joins
  [joins]
  (set (apply concat (vals joins))))

(defn polulate-columns-with-joins
  [{:keys [joins] :as floor-plan}]
  (update floor-plan :true-columns
    clojure.set/union (columns-in-joins joins)))

(defn columns-in-conditional-idents
  [conditional-idents]
  (set (vals conditional-idents)))

(defn polulate-columns-with-condititional-idents
  [{:keys [conditional-idents] :as floor-plan}]
  (update floor-plan :true-columns
    clojure.set/union (columns-in-conditional-idents conditional-idents)))

(defn polulate-cardinality-with-aggregators
  [{:keys [aggregators] :as floor-plan}]
  (update floor-plan :cardinality merge (zipmap (keys aggregators) (repeat :one))))

(defn expand-floor-plan-keys
  [{:keys [reversed-joins] :as floor-plan}]
  (-> floor-plan
    (update :true-columns set)
    (update :idents flatten-multi-keys)
    (update :extra-conditions (fnil flatten-multi-keys {}))
    (update :pagination-fallbacks (fnil flatten-multi-keys {}))
    (update :aggregators (fnil flatten-multi-keys {}))
    polulate-cardinality-with-aggregators
    (update :cardinality flatten-multi-keys)
    (update :joins (fnil flatten-multi-keys {}))
    polulate-columns-with-joins
    (update :joins expand-reversed-joins reversed-joins)
    (update :required-columns expand-denpendencies)))

(defn prepare-keywords
  [{:keys [true-columns aggregators pseudo-columns
           idents emitter joins] :as floor-plan}]
  (-> floor-plan
    (assoc :aggregator-keywords (set (keys aggregators)))

    (assoc :column-keywords
      (clojure.set/union true-columns
        (set (keys (merge aggregators pseudo-columns)))))

    (assoc :ident-keywords (set (keys idents)))
    (assoc :join-keywords (set (keys joins)))
    (dissoc :idents)))

(defn prepare-clojuric-names
  [{:keys [emitter column-keywords] :as floor-plan}]
  (-> floor-plan
    (assoc :clojuric-names (clojuric-names emitter column-keywords))))

(defn separate-floor-plan-keys
  [{:keys [joins emitter idents
           extra-conditions]
    :as floor-plan}]
  (-> floor-plan
    separate-idents
    polulate-columns-with-condititional-idents
    prepare-keywords
    prepare-clojuric-names))

(defn precompile-floor-plan
  [{:keys [joins emitter idents unconditional-idents conditional-idents] :as floor-plan}]
  (-> floor-plan
    (assoc :batch-query (emitter/emitter->batch-query emitter))
    (assoc :join-statements (compile-join-statements emitter joins))
    (assoc :target-tables (merge (conditional-idents->target-tables emitter conditional-idents)
                            (unconditional-idents->target-tables emitter unconditional-idents)
                            (joins->target-tables emitter joins)))
    (assoc :join-filter-subqueries (compile-join-filter-subqueries emitter joins))))

(def compile-floor-plan
  (comp internalize-keywords
    compile-floor-plan*
    precompile-floor-plan
    separate-floor-plan-keys
    expand-floor-plan-keys))
