(ns walkable.sql-query-builder
  (:require [walkable.sql-query-builder.filters :as filters]
            [clojure.spec.alpha :as s]
            [clojure.zip :as z]
            [com.wsscode.pathom.core :as p]))

(defn split-keyword
  "Splits a keyword into a tuple of table and column."
  [k]
  {:pre [(s/valid? ::filters/namespaced-keyword k)]
   :post [vector? #(= 2 (count %)) #(every? string? %)]}
  (->> ((juxt namespace name) k)
    (map #(-> % (clojure.string/replace #"-" "_")))))

(defn column-name
  "Converts a keyword to column name in full form (which means table
  name included) ready to use in an SQL query."
  [k]
  {:pre [(s/valid? ::filters/namespaced-keyword k)]
   :post [string?]}
  (->> (split-keyword k)
    (map #(str "`" % "`"))
    (clojure.string/join ".")))

(defn clojuric-name
  "Converts a keyword to an SQL alias"
  [k]
  {:pre [(s/valid? ::filters/namespaced-keyword k)]
   :post [string?]}
  (str "`" (subs (str k) 1) "`"))

(s/def ::keyword-string-map
  (s/coll-of (s/tuple ::filters/namespaced-keyword string?)))

(s/def ::keyword-keyword-map
  (s/coll-of (s/tuple ::filters/namespaced-keyword ::filters/namespaced-keyword)))

(defn ->column-names
  "Makes a hash-map of keywords and their equivalent column names"
  [ks]
  {:pre [(s/valid? (s/coll-of ::filters/namespaced-keyword) ks)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (zipmap ks
    (map column-name ks)))

(defn ->clojuric-names
  "Makes a hash-map of keywords and their Clojuric name (to be use as
  sql's SELECT aliases"
  [ks]
  {:pre [(s/valid? (s/coll-of ::filters/namespaced-keyword) ks)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (zipmap ks
    (map clojuric-name ks)))

(defn selection-with-aliases
  "Produces the part after `SELECT` and before `FROM <sometable>` of
  an SQL query"
  [{:keys [columns-to-query column-names clojuric-names]}]
  {:pre [(s/valid? (s/coll-of ::filters/namespaced-keyword) columns-to-query)
         (s/valid? ::keyword-string-map column-names)
         (s/valid? ::keyword-string-map clojuric-names)]
   :post [string?]}
  (->> columns-to-query
    (map (fn [column]
           (str (get column-names column)
             " AS "
             (get clojuric-names column))))
    (clojure.string/join ", ")))

(defn ->join-statement
  "Produces a SQL JOIN statement (of type string) given two pairs of
  table/column"
  [[[table-1 column-1] [table-2 column-2]]]
  {:pre [(every? string? [table-1 column-1 table-2 column-2])]
   :post [string?]}
  (str
    " JOIN `" table-2
    "` ON `"   table-1 "`.`" column-1 "` = `" table-2 "`.`" column-2 "`"))

(s/def ::no-join
  (s/coll-of ::filters/namespaced-keyword
    :count 2))

(s/def ::one-join
  (s/coll-of ::filters/namespaced-keyword
    :count 4))

(s/def ::join-seq
  (s/or
    :no-join  ::no-join
    :one-join ::one-join))

(defn split-join-seq
  [join-seq]
  {:pre [(s/valid? ::join-seq join-seq)]}
  (map split-keyword join-seq))

(defn ->join-statements
  "Helper for compile-schema. Generates JOIN statement strings for all
  join keys given their join sequence."
  [join-seq]
  {:pre [(s/valid? ::join-seq join-seq)]
   :post [string?]}
  (let [[tag] (s/conform ::join-seq join-seq)]
    (when (= :one-join tag)
      (->join-statement (map split-keyword (drop 2 join-seq))))))

(s/def ::joins
  (s/coll-of (s/tuple ::filters/namespaced-keyword ::join-seq)))

(defn source-column
  [join-seq]
  (first join-seq))

(defn target-column
  [join-seq]
  (second join-seq))

(defn target-table
  [join-seq]
  (first (split-keyword (target-column join-seq))))

(defn joins->target-tables
  "Produces map of join keys to their corresponding source table name."
  [joins]
  {:pre  [(s/valid? ::joins joins)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (target-table join-seq)))
    {} joins))

(defn joins->target-columns
  "Produces map of join keys to their corresponding target column."
  [joins]
  {:pre  [(s/valid? ::joins joins)]
   :post [#(s/valid? ::keyword-keyword-map %)]}
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (target-column join-seq)))
    {} joins))

(defn joins->source-columns
  "Produces map of join keys to their corresponding source column."
  [joins]
  {:pre  [(s/valid? ::joins joins)]
   :post [#(s/valid? ::keyword-keyword-map %)]}
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (source-column join-seq)))
    {} joins))

(s/def ::query-string-input
  (s/keys :req-un [::columns-to-query ::column-names ::clojuric-names ::target-table]
    :opt-un [::join-statement ::where-conditions
             ::offset ::limit ::order-by]))

(defn ->query-string
  "Builds the final query string ready for SQL server."
  [{:keys [columns-to-query column-names clojuric-names target-table
           join-statement where-conditions
           offset limit order-by] :as input}]
  {:pre  [(s/valid? ::query-string-input input)]
   :post [string?]}
  (str "SELECT "
    (selection-with-aliases {:columns-to-query columns-to-query
                             :column-names     column-names
                             :clojuric-names   clojuric-names})
    " FROM `" target-table "`"

    join-statement

    (when where-conditions
      (str " WHERE "
        where-conditions))
    (when order-by
      (str " ORDER BY " order-by))
    (when limit
      (str " LIMIT " limit))
    (when offset
      (str " OFFSET " offset))))

(s/def ::sql-schema
  (s/keys :req []
    :opt []))

(defn ast-zipper
  "Make a zipper to navigate an ast tree possibly with placeholder
  subtrees."
  [ast {:keys [leaf? placeholder?]}]
  (->> ast
    (z/zipper
      (fn branch? [x] (and (map? x)
                        (or (= :root (::my-marker x)) (placeholder? x))
                        (seq (:children x))))
      (fn children [x] (->> (:children x) (filter #(or (leaf? %) (placeholder? %)))))
      (fn make-node [x xs] (assoc x :children (vec xs))))))

(defn all-zipper-children
  "Given a zipper, returns all its children"
  [zipper]
  (->> zipper
    (iterate z/next)
    rest ;; remove the root itself
    (take-while #(not (z/end? %)))))

(defn find-all-children
  "Find all direct children, or children in nested placeholders."
  [ast {:keys [placeholder? leaf?]}]
  (->>
    (ast-zipper (assoc ast ::my-marker :root)
      {:leaf?        leaf?
       :placeholder? placeholder?})
    (all-zipper-children)
    (map z/node)
    rest
    (remove placeholder?)))

(defn process-children
  "Infers which columns to include in SQL query from child keys in env ast"
  [{::keys [sql-schema] :keys [ast] ::p/keys [placeholder-prefixes]
    :as env}]
  {:pre  [(s/valid? ::sql-schema sql-schema)]
   :post [#(s/valid? (s/keys :req-un [::child-join-keys ::columns-to-query]) %)]}
  (let [{::keys [column-keywords required-columns source-columns]} sql-schema

        all-children
        (find-all-children ast
          {:placeholder? #(contains? placeholder-prefixes
                            (namespace (:dispatch-key %)))
           :leaf? #(or (contains? column-keywords (:dispatch-key %))
                     (contains? source-columns (:dispatch-key %))
                     (contains? required-columns (:dispatch-key %)))})

        {:keys [column-children join-children]}
        (->> all-children
          (group-by #(cond (contains? source-columns (:dispatch-key %))
                           :join-children

                           (contains? column-keywords (:dispatch-key %))
                           :column-children)))

        all-child-keys
        (map :dispatch-key column-children)

        child-column-keys
        (map :dispatch-key column-children)

        child-required-keys
        (->> all-child-keys (map #(get required-columns %)) (apply clojure.set/union))

        child-join-keys
        (map :dispatch-key join-children)

        child-source-columns
        (->> child-join-keys (map #(get source-columns %)) (into #{}))]
    {:child-join-keys  child-join-keys
     :columns-to-query (clojure.set/union
                         child-column-keys
                         child-required-keys
                         child-source-columns)}))

(defn get-child-env
  [{:keys [ast] :as env} child-join-key]
  (let [children (->> ast :children
                   (some #(when (= child-join-key (:dispatch-key %)) %)))]
    (assoc env :ast children)))

(s/def ::conditional-ident
  (s/tuple keyword? (s/tuple ::filters/operators ::filters/namespaced-keyword)))

(defn conditional-idents->target-tables
  "Produces map of ident keys to their corresponding source table name."
  [idents]
  {:pre  [(s/valid? (s/coll-of ::conditional-ident) idents)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (reduce (fn [result [ident-key [_operator column-keyword]]]
            (assoc result ident-key
              (first (split-keyword column-keyword))))
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

(s/def ::ident-condition
  (s/tuple ::filters/operators ::filters/namespaced-keyword))

(defn ident->condition
  "Converts given ident key in env to equivalent condition dsl."
  [env condition]
  {:pre  [(s/valid? ::ident-condition condition)
          (s/valid? (s/keys :req-un [::ast]) env)]
   :post [#(s/valid? ::filters/clauses %)]}
  (let [params         (-> env :ast :key rest)
        [operator key] condition]
    {key (cons operator params)}))

(defn compile-extra-conditions
  [extra-conditions]
  (reduce (fn [result [k v]]
            (assoc result k
              (if (fn? v)
                v
                (fn [env] v))))
    {} extra-conditions))

(defn compile-join-statements
  [joins]
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (->join-statements join-seq)))
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

(defn separate-idents [idents]
  (reduce (fn [result [k v]]
            (if (string? v)
              (assoc-in result [:unconditional-idents k] v)
              (assoc-in result [:conditional-idents k] v)))
    {:unconditional-idents {}
     :conditional-idents   {}}
    idents))

(defn compile-schema
  [{:keys [columns pseudo-columns required-columns idents extra-conditions
           reversed-joins joins join-cardinality]}]
  (let [idents                                            (flatten-multi-keys idents)
        {:keys [unconditional-idents conditional-idents]} (separate-idents idents)
        extra-conditions                                  (flatten-multi-keys extra-conditions)
        joins                                             (->> (flatten-multi-keys joins)
                                                            (expand-reversed-joins reversed-joins))
        join-cardinality                                  (flatten-multi-keys join-cardinality)
        true-columns                                      (set (apply concat columns (vals joins)))
        columns                                           (set (concat true-columns
                                                                 (keys pseudo-columns)))]
    #::{:column-keywords  columns
        :ident-keywords   (set (keys idents))
        :required-columns (expand-denpendencies required-columns)
        :target-tables    (merge (conditional-idents->target-tables conditional-idents)
                            unconditional-idents
                            (joins->target-tables joins))
        :target-columns   (joins->target-columns joins)
        :source-columns   (joins->source-columns joins)

        :join-cardinality join-cardinality
        :ident-conditions conditional-idents
        :extra-conditions (compile-extra-conditions extra-conditions)
        :column-names     (merge (->column-names true-columns)
                            pseudo-columns)
        :clojuric-names   (->clojuric-names columns)
        :join-statements  (compile-join-statements joins)}))

(defn clean-up-all-conditions
  [all-conditions]
  (let [all-conditions (remove nil? all-conditions)]
    (case (count all-conditions)
      0 nil
      1 (first all-conditions)
      all-conditions)))

(defn process-pagination
  [{::keys [sql-schema] :as env}]
  {:pre [(s/valid? (s/keys :req [::column-names]) sql-schema)]
   :post [#(s/valid? (s/keys :req-un [::offset ::limit ::order-by]))]}
  (let [{::keys [column-names]} sql-schema]
    {:offset
     (when-let [offset (get-in env [:ast :params ::offset])]
       (when (integer? offset)
         offset))
     :limit
     (when-let [limit (get-in env [:ast :params ::limit])]
       (when (integer? limit)
         limit))
     :order-by
     (when-let [order-by (get-in env [:ast :params ::order-by])]
       (filters/->order-by-string column-names order-by))}))

(defn process-conditions
  [{::keys [sql-schema] :as env}]
  (let [{::keys [ident-conditions extra-conditions target-columns source-columns]}
        sql-schema
        e (p/entity env)
        k (get-in env [:ast :dispatch-key])

        ident-condition
        (when-let [condition (get ident-conditions k)]
          (ident->condition env condition))

        target-column
        (get target-columns k)

        source-column
        (get source-columns k)

        target-condition
        (when target-column ;; if it's a join
          {target-column
           [:= (get e source-column)]})

        extra-condition
        (when-let [->condition (get extra-conditions k)]
          (->condition env))

        supplied-condition
        (get-in env [:ast :params ::filters])

        supplied-condition
        (when (s/valid? ::filters/clauses supplied-condition)
          supplied-condition)]
    [ident-condition target-condition extra-condition supplied-condition]))

(defn parameterize-all-conditions
  [{::keys [sql-schema] :as env}]
  (let [{::keys [clojuric-names]} sql-schema
        all-conditions          (clean-up-all-conditions (process-conditions env))]
    (when all-conditions
      (filters/parameterize {:key    nil
                             :keymap clojuric-names}
        all-conditions))))

(defn process-query
  [{::keys [sql-schema] :as env}]
  (let [{::keys [column-keywords
                 column-names
                 clojuric-names
                 join-statements
                 target-tables
                 target-columns]}                  sql-schema
        k                                          (get-in env [:ast :dispatch-key])
        [where-conditions query-params]            (parameterize-all-conditions env)
        {:keys [child-join-keys columns-to-query]} (process-children env)
        columns-to-query                           (if-let [target-column (get target-columns k)]
                                                     (conj columns-to-query target-column)
                                                     columns-to-query)
        {:keys [offset limit order-by]}            (process-pagination env)]
    {:query-string-input {:target-table        (get target-tables k)
                          :join-statement      (get join-statements k)
                          :columns-to-query    columns-to-query
                          :column-names        column-names
                          :clojuric-names      clojuric-names
                          :where-conditions    where-conditions
                          :offset              offset
                          :limit               limit
                          :order-by            order-by}
     :query-params       query-params
     :child-join-keys    child-join-keys}))

(defn batch-query
  "Combines multiple SQL queries and their params into a single query
  using UNION."
  [query-strings params]
  (let [union-query (clojure.string/join "\nUNION\n"
                      (map #(str "SELECT * FROM (" % ")")
                        query-strings))]
    (cons union-query (apply concat params))))

(defn pull-entities
  [{::keys [sql-schema sql-db run-query] :as env}]
  (let [{::keys [ident-keywords
                 target-tables
                 target-columns
                 source-columns
                 join-statements
                 join-cardinality]} sql-schema
        k                           (get-in env [:ast :dispatch-key])]
    (if (contains? target-tables k)
      ;; this is an ident or a join, let's go for data
      (let [{:keys [query-string-input query-params child-join-keys]}
            (process-query env)

            query-string
            (when (contains? ident-keywords k)
              (->query-string query-string-input))

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
                (get parent k)))

            ;;join-child-queries
            join-children-data-by-join-key
            (when (seq child-join-keys)
              (into {}
                (for [j child-join-keys]
                  (let [;; (let [j (:dispatch-key child-join)]
                        ;; parent
                        source-column (get source-columns j)
                        ;; children
                        target-column (get target-columns j)

                        query-string-inputs
                        (for [e entities]
                          (process-query
                            (assoc-in (get-child-env env j)
                              [::p/entity source-column] (get e source-column))))

                        query-strings (map #(->query-string (:query-string-input %)) query-string-inputs)
                        all-params        (map :query-params query-string-inputs)

                        join-children-data
                        (run-query sql-db (batch-query query-strings all-params))]
                    [j (group-by target-column join-children-data)]))))

            entities-with-join-children-data
            (for [e entities]
              (let [child-joins
                    (into {}
                      (for [j child-join-keys]
                        (let [source-column (get source-columns j)
                              parent-id     (get e source-column)
                              children      (get-in join-children-data-by-join-key
                                              [j parent-id])]
                          [j children])))]
                (merge e child-joins)))

            one?
            (= :one (get join-cardinality k))

            do-join
            (if one?
              #(p/join (first %2) %1)
              #(p/join-seq %1 %2))]
        (if (seq entities-with-join-children-data)
          (do-join env entities-with-join-children-data)
          (when-not one?
            [])))

      ::p/continue)))
