(ns walkable.sql-query-builder.floor-plan
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.pathom-env :as env]
            [com.wsscode.pathom.core :as p]
            [clojure.spec.alpha :as s]))

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

(defn compile-pagination-fallbacks
  [clojuric-names pagination-fallbacks]
  (reduce (fn [result [k {:keys [offset limit order-by]}]]
            (assoc result
              k
              {:offset-fallback   (pagination/offset-fallback offset)
               :limit-fallback    (pagination/limit-fallback limit)
               :order-by-fallback (pagination/order-by-fallback
                                    (pagination/conform-fallback-default clojuric-names order-by))}))
    {} pagination-fallbacks))

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

(defn expand-reversed-joins [reversed-joins joins]
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
       :source-columns (joins->source-columns joins)})
    (dissoc :idents)))

(defn unbound-expression?
  [compiled-expression]
  (boolean (some expressions/atomic-variable? (:params compiled-expression))))

(comment
  (false? (unbound-expression? {:raw-string "abc"
                                :params []}))
  (false? (unbound-expression? {:raw-string "abc AND ?"
                                :params ["bla"]}))
  (true? (unbound-expression? {:raw-string "abc AND ?"
                               :params [(expressions/av :x/a)]})))

(defn rotate [coll]
  (take (count coll) (drop 1 (cycle coll))))

(comment
  (= (rotate [:a :b :c :d]) [:b :c :d :a])
  (= (rotate [:a :b]) [:b :a])
  (= (rotate []) [])
  )

(defn compile-formulas-once [compiled-true-columns formulas]
  (reduce-kv (fn [result k expression]
               (let [compiled (expressions/compile-to-string {} expression)]
                 (if (unbound-expression? compiled)
                   (update result :unbound assoc k compiled)
                   (update result :bound assoc k compiled))))
    {:unbound {}
     :bound   compiled-true-columns}
    formulas))

(comment
  (= (compile-formulas-once (compile-true-columns emitter/postgres-emitter #{:x/a :x/b})
       {:x/c 99
        :x/d [:- 100 :x/c]})
    {:unbound #:x {:d {:params     [#walkable.sql_query_builder.expressions.AtomicVariable{:name :x/c}],
                       :raw-string "(100)-(?)"}},
     :bound   #:x {:a {:raw-string "\"x\".\"a\"", :params []},
                   :b {:raw-string "\"x\".\"b\"", :params []},
                   :c {:raw-string "99", :params []}}}))

(defn compile-formulas-recursively [{:keys [unbound bound]}]
  (loop [limit   100
         unbound (seq unbound)
         bound   bound]
    (if-let [item (and (pos? limit) (first unbound))]
      (let [[k compiled-expression] item

            attempt (expressions/substitute-atomic-variables
                      {:variable-values bound}
                      compiled-expression)]
        (if (unbound-expression? attempt)
          (recur (dec limit)
            (rotate unbound)
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

(comment
  (= (column-dependencies
      (compile-formulas-recursively (compile-formulas-once (compile-true-columns emitter/postgres-emitter #{:x/a :x/b})
                  {:x/c [:+ :x/d (expressions/av 'o)]
                   :x/d [:- 100 :x/e]
                   :x/e [:- 100 :x/c]})))
    #:x{:d #{:x/e}, :e #{:x/c}, :c #{:x/d}})
  )

(defn compile-selection [compiled-formula clojuric-name]
  (expressions/inline-params {}
    {:raw-string "(?) AS ?"
     :params     [compiled-formula
                  (expressions/verbatim-raw-string clojuric-name)]}))
(comment
  (= (compile-selection
       {:raw-string "? - `human`.`yob` ?"
        :params     [(expressions/av 'current-year)]}
       "`human/age`")
    {:params     [#walkable.sql_query_builder.expressions.AtomicVariable{:name current-year}],
     :raw-string "(? - `human`.`yob` ?) AS `human/age`"}))

(defn compile-variable-getters
  [getters]
  (->> getters
    (map (fn [{function :fn k :key :keys [cached?]}]
           [k
            (if cached?
              (fn [env]
                (p/cached env [:walkable/variable-getter k]
                  (function env)))
              function)]))
    (into {})))

(defn check-column-vars
  [column-vars])

(comment
  (= (compile-formulas-recursively (compile-formulas-once (compile-true-columns emitter/postgres-emitter #{:x/a :x/b})
                 {:x/c 99
                  :x/d [:- 100 :x/c]}))
    {:unbound {},
     :bound   #:x {:a {:raw-string "\"x\".\"a\"", :params []},
                   :b {:raw-string "\"x\".\"b\"", :params []},
                   :c {:raw-string "99", :params []},
                   :d {:raw-string "(100)-(99)", :params []}}}))

(comment
  {:variable-getters [{:cached?  true
                       :key         'abc
                       :fn          (fn [env] :bla)}]}
  (require '[loom.graph :as g])
  (require '[loom.alg :as ga])
  (ga/dag? (g/digraph [1 2] [2 3] [3 1]))
  )

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
                           [:= ident-key (expressions/av `ident-key-value)]))))
          {}
          conditional-idents)]
    (assoc floor-plan :compiled-ident-conditions compiled-ident-conditions)))

(defn compile-join-conditions
  [{:keys [joins compiled-formulas target-columns] :as floor-plan}]
  (let [compiled-join-conditions
        (reduce-kv (fn [acc k join-seq]
                     (let [target-column (get target-columns k)]
                       (assoc acc k
                         (expressions/substitute-atomic-variables
                           {:variable-values compiled-formulas}
                           (expressions/compile-to-string {}
                             [:= target-column (expressions/av `source-column-value)])))))
          {}
          joins)]
    (assoc floor-plan :compiled-join-conditions compiled-join-conditions)))
(defn compile-floor-plan*
  "Given a brief user-supplied floor-plan, derives an efficient floor-plan
  ready for pull-entities to use."
  [{:keys [true-columns column-keywords clojuric-names
           stateless-formulas stateful-formulas
           required-columns
           ident-keywords
           emitter

           stateless-formulas stateful-formulas
           stateless-conditions stateful-conditions
           pagination-fallbacks
           reversed-joins joins cardinality
           aggregator-keywords
           batch-query
           unconditional-idents conditional-idents
           join-statements target-tables
           join-filter-subqueries
           target-columns source-columns]
    :or {emitter              emitter/default-emitter
         aggregators          {}
         extra-conditions     {}
         pagination-fallbacks {}
         joins                {}
         cardinality          {}}
    :as input-floor-plan}]
  (merge #::{:cardinality            cardinality
             :emitter                emitter
             :batch-query            batch-query
             :target-tables          target-tables
             :join-filter-subqueries join-filter-subqueries
             :target-columns         target-columns
             :source-columns         source-columns
             :ident-conditions       conditional-idents
             :required-columns       required-columns
             :join-statements        join-statements
             :aggregator-keywords    aggregator-keywords
             :stateless-formulas     stateless-formulas
             :stateful-formulas      stateful-formulas
             :stateless-conditions   stateless-conditions
             :stateful-conditions    stateful-conditions
             :column-keywords        column-keywords
             :true-columns           true-columns
             :ident-keywords         ident-keywords
             :clojuric-names         clojuric-names
             :pagination-fallbacks   (compile-pagination-fallbacks clojuric-names pagination-fallbacks)}))

(defn columns-in-joins
  [joins]
  (set (apply concat (vals joins))))

(defn polulate-columns-with-joins
  [{:keys [joins] :as floor-plan}]
  (update floor-plan :true-columns
    clojure.set/union (columns-in-joins joins)))

(comment
  (columns-in-joins {:x [:u :v] :y [:m :n]})
  (= (polulate-columns-with-joins {:joins       {:x [:u :v] :y [:m :n]}
                                   :true-columns #{:a :b}})
    {:joins        {:x [:u :v], :y [:m :n]},
     :true-columns #{:v :n :m :b :a :u}}))

(defn columns-in-conditional-idents
  [conditional-idents]
  (set (vals conditional-idents)))

(defn polulate-columns-with-condititional-idents
  [{:keys [conditional-idents] :as floor-plan}]
  (update floor-plan :true-columns
    clojure.set/union (columns-in-conditional-idents conditional-idents)))

(comment
  (columns-in-conditional-idents {:x/by-id :x/id :y/by-id :y/id})
  (= (polulate-columns-with-condititional-idents {:conditional-idents {:x/by-id :x/id :y/by-id :y/id}
                                                  :true-columns       #{:x/id :m/id}})
    {:conditional-idents {:x/by-id :x/id, :y/by-id :y/id},
     :true-columns #{:m/id :y/id :x/id}}))

(defn expand-floor-plan-keys
  [{:keys [reversed-joins aggregators] :as floor-plan}]
  (-> floor-plan
    (update :true-columns set)
    (update :idents flatten-multi-keys)
    (update :extra-conditions flatten-multi-keys)
    (update :pagination-fallbacks flatten-multi-keys)
    (update :aggregators flatten-multi-keys)
    (update :cardinality merge (zipmap (keys aggregators) (repeat :one)))
    (update :cardinality flatten-multi-keys)
    (update :joins flatten-multi-keys)
    polulate-columns-with-joins
    (update :joins #(expand-reversed-joins reversed-joins %))
    (update :required-columns expand-denpendencies)))

(defn compile-true-columns
  "Makes a hash-map of keywords and their equivalent compiled form."
  [emitter ks]
  (zipmap ks
    (map #(expressions/verbatim-raw-string (emitter/column-name emitter %)) ks)))

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

(def compile-floor-plan (comp expand-floor-plan-keys
                          separate-floor-plan-keys
                          precompile-floor-plan
                          compile-floor-plan*))
