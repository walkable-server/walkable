(ns walkable.sql-query-builder.floor-plan
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.pathom-env :as env]
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

(defn separate-extra-conditions*
  [extra-conditions]
  (reduce (fn [result [k v]]
            (if (fn? v)
              (assoc-in result [:stateful-conditions k] v)
              (assoc-in result [:stateless-conditions k] v)))
    {:stateless-conditions {}
     :stateful-conditions  {}}
    extra-conditions))

(defn separate-extra-conditions
  [{:keys [extra-conditions] :as floor-plan}]
  (-> floor-plan
    (merge (separate-extra-conditions* extra-conditions))
    (dissoc :extra-conditions)))

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

(defn separate-formulas*
  "Helper function for compile-floor-plan. Separates pseudo-columns
  to stateless formulas and stateful formulas for further processing."
  [formulas]
  (-> (reduce (fn [result [k v]]
                (if (fn? v)
                  (assoc-in result [:stateful-formulas k] v)
                  (assoc-in result [:stateless-formulas k] v)))
        {:stateless-formulas {}
         :stateful-formulas  {}}
        formulas)))

(defn separate-formulas
  [{:keys [pseudo-columns aggregators] :as floor-plan}]
  (-> floor-plan
    (merge (separate-formulas* (merge aggregators pseudo-columns)))))

(s/def ::floor-plan
  (s/keys :req [::column-keywords
                ::target-columns
                ::extra-conditions
                ::pagination-fallbacks
                ::join-statements
                ::join-filter-subqueries
                ::required-columns
                ::clojuric-names
                ::column-names
                ::ident-keywords
                ::source-columns
                ::ident-conditions
                ::cardinality
                ::emitter
                ::target-tables
                ::aggregators
                ::batch-query]))

(defn compile-floor-plan*
  "Given a brief user-supplied floor-plan, derives an efficient floor-plan
  ready for pull-entities to use."
  [{:keys [columns pseudo-columns required-columns idents
           emitter extra-conditions pagination-fallbacks
           reversed-joins joins cardinality
           aggregators batch-query
           unconditional-idents conditional-idents
           join-statements
           target-columns source-columns]
    :or   {emitter              emitter/default-emitter
           aggregators          {}
           extra-conditions     {}
           pagination-fallbacks {}
           joins                {}
           cardinality          {}}
    :as   input-floor-plan}]
  (merge #::{:cardinality      cardinality
             :emitter          emitter
             :batch-query      batch-query
             :target-columns   target-columns
             :source-columns   source-columns
             :ident-conditions conditional-idents
             :required-columns required-columns
             :join-statements  join-statements
             :extra-conditions extra-conditions}
    (let [true-column-keywords (set (apply concat columns (vals joins)))
          true-columns         (column-names emitter true-column-keywords)
          columns              (set (concat true-column-keywords
                                      (keys pseudo-columns)))
          clojuric-names       (clojuric-names emitter
                                 (concat columns (keys aggregators)))
          static-columns       (column-names emitter true-column-keywords)]
      #::{:column-keywords columns
          :ident-keywords  (set (keys idents))
          :target-tables   (merge (conditional-idents->target-tables emitter conditional-idents)
                             (unconditional-idents->target-tables emitter unconditional-idents)
                             (joins->target-tables emitter joins))

          :pagination-fallbacks    (compile-pagination-fallbacks clojuric-names pagination-fallbacks)
          :static-columns          static-columns
          :dynamic-column-keywords (set (keys pseudo-columns))
          :dynamic-columns         pseudo-columns
          ;; todo: rename to `:processed-columns`
          :column-names            (merge true-columns pseudo-columns aggregators)
          :processed-columns       (merge true-columns pseudo-columns aggregators)
          :clojuric-names          clojuric-names
          :aggregator-keywords     (set (keys aggregators))
          :join-filter-subqueries  (compile-join-filter-subqueries emitter joins)})))

(defn expand-floor-plan
  [{:keys [reversed-joins aggregators] :as floor-plan}]
  (-> floor-plan
    (update :idents flatten-multi-keys)
    (update :extra-conditions flatten-multi-keys)
    (update :pagination-fallbacks flatten-multi-keys)
    (update :aggregators flatten-multi-keys)
    (update :cardinality merge (zipmap (keys aggregators) (repeat :one)))
    (update :cardinality flatten-multi-keys)
    (update :joins flatten-multi-keys)
    (update :joins #(expand-reversed-joins reversed-joins %))
    (update :required-columns expand-denpendencies)
    (update :extra-conditions compile-extra-conditions)))

(defn expand-more
  [{:keys [joins emitter idents] :as floor-plan}]
  (-> floor-plan
    (assoc :batch-query (emitter/emitter->batch-query emitter))
    (assoc :join-statements (compile-join-statements emitter joins))
    (merge (separate-idents idents)
      {:target-columns (joins->target-columns joins)
       :source-columns (joins->source-columns joins)})))

(def new-compile-floor-plan (comp expand-floor-plan expand-more compile-floor-plan*))
