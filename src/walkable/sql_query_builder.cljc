(ns walkable.sql-query-builder
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.pathom-env :as env]
            [clojure.spec.alpha :as s]
            [clojure.zip :as z]
            [clojure.core.async :refer [go go-loop <! >! put! promise-chan to-chan]]
            [com.wsscode.pathom.core :as p]))

(def backticks
  (repeat 2 "`"))

(def quotation-marks
  (repeat 2 "\""))

(def apostrophes
  (repeat 2 "'"))

(defn split-keyword
  "Splits a keyword into a tuple of table and column."
  [k]
  {:pre [(s/valid? ::expressions/namespaced-keyword k)]
   :post [vector? #(= 2 (count %)) #(every? string? %)]}
  (->> ((juxt namespace name) k)
    (map #(-> % (clojure.string/replace #"-" "_")))))

(defn column-name
  "Converts a keyword to column name in full form (which means table
  name included) ready to use in an SQL query."
  [[quote-open quote-close] k]
  {:pre [(s/valid? ::expressions/namespaced-keyword k)]
   :post [string?]}
  (->> (split-keyword k)
    (map #(str quote-open % quote-close))
    (clojure.string/join ".")))

(defn clojuric-name
  "Converts a keyword to an SQL alias"
  [[quote-open quote-close] k]
  {:pre [(s/valid? ::expressions/namespaced-keyword k)]
   :post [string?]}
  (str quote-open (subs (str k) 1) quote-close))

(s/def ::keyword-string-map
  (s/coll-of (s/tuple ::expressions/namespaced-keyword string?)))

(s/def ::keyword-keyword-map
  (s/coll-of (s/tuple ::expressions/namespaced-keyword ::expressions/namespaced-keyword)))

(defn ->column-names
  "Makes a hash-map of keywords and their equivalent column names"
  [quote-marks ks]
  {:pre [(s/valid? (s/coll-of ::expressions/namespaced-keyword) ks)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (zipmap ks
    (map #(column-name quote-marks %) ks)))

(defn ->clojuric-names
  "Makes a hash-map of keywords and their Clojuric name (to be use as
  sql's SELECT aliases"
  [quote-marks ks]
  {:pre [(s/valid? (s/coll-of ::expressions/namespaced-keyword) ks)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (zipmap ks
    (map #(clojuric-name quote-marks %) ks)))

(defn ->join-statement
  "Produces a SQL JOIN statement (of type string) given two pairs of
  table/column"
  [{:keys [quote-marks joins]}]
  {:post [string?]}
  (let [[[table-1 column-1] [table-2 column-2]] joins
        [quote-open quote-close]                quote-marks]
    (assert (every? string? [table-1 column-1 table-2 column-2]))
    (str
      " JOIN " quote-open table-2 quote-close
      " ON "   quote-open table-1 quote-close "." quote-open column-1 quote-close
      " = "    quote-open table-2 quote-close "." quote-open column-2 quote-close)))

(s/def ::no-join
  (s/coll-of ::expressions/namespaced-keyword
    :count 2))

(s/def ::one-join
  (s/coll-of ::expressions/namespaced-keyword
    :count 4))

(s/def ::join-seq
  (s/or
    :no-join  ::no-join
    :one-join ::one-join))

(defn ->join-statements
  "Helper for compile-schema. Generates JOIN statement strings for all
  join keys given their join sequence."
  [quote-marks join-seq]
  {:pre  [(s/valid? ::join-seq join-seq)]
   :post [string?]}
  (let [[tag] (s/conform ::join-seq join-seq)]
    (when (= :one-join tag)
      (->join-statement {:quote-marks quote-marks
                         :joins       (map split-keyword (drop 2 join-seq))}))))

(s/def ::join-specs
  (s/coll-of (s/tuple ::expressions/namespaced-keyword ::join-seq)))

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
  {:pre  [(s/valid? ::join-specs joins)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (target-table join-seq)))
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

(s/def ::query-string-input
  (s/keys :req-un [::selection ::target-table ::quote-marks]
    :opt-un [::join-statement ::where-conditions
             ::offset ::limit ::order-by]))

(defn ->query-string
  "Builds the final query string ready for SQL server."
  [{:keys [selection target-table
           join-statement where-conditions
           offset limit order-by quote-marks] :as input}]

  {:pre  [(s/valid? ::query-string-input input)]
   :post [string?]}
  (let [[quote-open quote-close] quote-marks]
    (str "SELECT " selection
      " FROM " quote-open target-table quote-close

      join-statement

      (when where-conditions
        (str " WHERE "
          where-conditions))
      (when order-by
        (str " ORDER BY " order-by))
      (when limit
        (str " LIMIT " limit))
      (when offset
        (str " OFFSET " offset)))))

(defn join-filter-subquery
  [quote-marks joins]
  (str
    (column-name quote-marks (source-column joins))
    " IN ("
    (->query-string {:selection      (column-name quote-marks (target-column joins))
                     :target-table   (target-table joins)
                     :quote-marks    quote-marks
                     :join-statement (->join-statements quote-marks joins)})
    " WHERE ?)"))

(defn ast-root
  [ast]
  (assoc ast ::my-marker :root))

(defn ast-zipper-root?
  [x]
  (= :root (::my-marker x)))

(s/def ::zipper-fns
  (s/keys :req-un [::placeholder? ::leaf?]))

(defn ast-zipper
  "Make a zipper to navigate an ast tree possibly with placeholder
  subtrees."
  [ast {:keys [placeholder? leaf?] :as zipper-fns}]
  {:pre [(map? ast) (s/valid? ::zipper-fns zipper-fns)]}
  (->> ast
    (z/zipper
      (fn branch? [x] (and (map? x)
                        (or (ast-zipper-root? x) (placeholder? x))
                        (seq (:children x))))
      (fn children [x] (->> (:children x) (filter #(or (leaf? %) (placeholder? %)))))
      ;; not neccessary because we only want to read, not write
      (fn make-node [x xs] (assoc x :children (vec xs))))))

(defn all-zipper-children
  "Given a zipper, returns all its children"
  [zipper]
  (->> zipper
    (iterate z/next)
    (take-while #(not (z/end? %)))))

(defn find-all-children
  "Find all direct children, or children in nested placeholders."
  [ast {:keys [placeholder? leaf?] :as zipper-fns}]
  {:pre [(map? ast) (s/valid? ::zipper-fns zipper-fns)]}
  (->>
    (ast-zipper (ast-root ast)
      {:leaf?        leaf?
       :placeholder? placeholder?})
    (all-zipper-children)
    (map z/node)
    (remove #(or (ast-zipper-root? %) (placeholder? %)))))

(s/def ::sql-schema
  (s/keys :req [::column-keywords
                ::target-columns
                ::extra-conditions
                ::join-statements
                ::join-filter-subqueries
                ::required-columns
                ::clojuric-names
                ::column-names
                ::ident-keywords
                ::source-columns
                ::ident-conditions
                ::cardinality
                ::quote-marks
                ::target-tables
                ::batch-query]))

(defn process-children
  "Infers which columns to include in SQL query from child keys in env ast"
  [{::keys [sql-schema] :keys [ast] ::p/keys [placeholder-prefixes]
    :as    env}]
  {:pre  [(s/valid? (s/keys :req [::column-keywords ::source-columns]
                      :opt [::required-columns])
            sql-schema)

          (if placeholder-prefixes
            (set? placeholder-prefixes)
            true)]
   :post [#(s/valid? (s/keys :req-un [::join-children ::columns-to-query]) %)]}
  (let [{::keys [column-keywords required-columns source-columns]} sql-schema

        all-children
        (find-all-children ast
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

(s/def ::conditional-ident
  (s/tuple keyword? ::expressions/namespaced-keyword))

(defn conditional-idents->target-tables
  "Produces map of ident keys to their corresponding source table name."
  [idents]
  {:pre  [(s/valid? (s/coll-of ::conditional-ident) idents)]
   :post [#(s/valid? ::keyword-string-map %)]}
  (reduce (fn [result [ident-key column-keyword]]
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

(defn ident->condition
  "Converts given ident key in env to equivalent condition dsl."
  [env key]
  {:pre  [(s/valid? ::expressions/namespaced-keyword key)
          (s/valid? (s/keys :req-un [::ast]) env)]
   :post [#(s/valid? ::expressions/expression %)]}
  (let [params (-> env :ast :key rest)]
    (vec (concat [:= key] params))))

(defn compile-extra-conditions
  [extra-conditions]
  (reduce (fn [result [k v]]
            (assoc result k
              (if (fn? v)
                v
                (fn [env] v))))
    {} extra-conditions))

(defn compile-join-statements
  [quote-marks joins]
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (->join-statements quote-marks join-seq)))
    {} joins))

(defn compile-join-filter-subqueries
  [quote-marks joins]
  (reduce (fn [result [k join-seq]]
            (assoc result k
              (join-filter-subquery quote-marks join-seq)))
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

(defn separate-idents
  "Helper function for compile-schema. Separates all user-supplied
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

(defn batch-query
  "Combines multiple SQL queries and their params into a single query
  using UNION."
  [query-strings params]
  (let [union-query (clojure.string/join "\nUNION ALL\n"
                      query-strings)]
    (cons union-query (apply concat params))))

(defn wrap-select
  "Wrap a SQL string in (...)"
  [s]
  (str "(" s ")"))

(defn wrap-select-sqlite
  "Wrap a SQL string in SELECT * FROM (...)"
  [s]
  (str "SELECT * FROM (" s ")"))

(defn compile-schema
  "Given a brief user-supplied schema, derives an efficient schema
  ready for pull-entities to use."
  [{:keys [columns pseudo-columns required-columns idents extra-conditions
           reversed-joins joins cardinality quote-marks sqlite-union]
    :or   {quote-marks      backticks
           extra-conditions {}
           joins            {}
           cardinality      {}}
    :as   input-schema}]

  {:pre  [(s/valid? (s/keys :req-un [::columns ::idents]
                      :opt-un [::pseudo-columns ::required-columns ::extra-conditions
                               ::sqlite-union ::quote-marks
                               ::reversed-joins ::joins ::cardinality])
            input-schema)]
   :post [#(s/valid? ::sql-schema %)]}
  (let [idents                                            (flatten-multi-keys idents)
        {:keys [unconditional-idents conditional-idents]} (separate-idents idents)
        extra-conditions                                  (flatten-multi-keys extra-conditions)
        joins                                             (->> (flatten-multi-keys joins)
                                                            (expand-reversed-joins reversed-joins))
        cardinality                                       (flatten-multi-keys cardinality)
        true-columns                                      (set (apply concat columns (vals joins)))
        columns                                           (set (concat true-columns
                                                                 (keys pseudo-columns)))]
    #::{:column-keywords  columns
        :ident-keywords   (set (keys idents))
        :quote-marks      quote-marks
        :required-columns (expand-denpendencies required-columns)
        :target-tables    (merge (conditional-idents->target-tables conditional-idents)
                            unconditional-idents
                            (joins->target-tables joins))
        :target-columns   (joins->target-columns joins)
        :source-columns   (joins->source-columns joins)

        :batch-query      (fn [query-strings params]
                            (batch-query (map (if sqlite-union
                                                wrap-select-sqlite
                                                wrap-select)
                                           query-strings) params))

        :cardinality      cardinality
        :ident-conditions conditional-idents
        :extra-conditions (compile-extra-conditions extra-conditions)
        :column-names     (merge (->column-names quote-marks true-columns)
                            pseudo-columns)
        :clojuric-names   (->clojuric-names quote-marks columns)
        :join-statements  (compile-join-statements quote-marks joins)
        :join-filter-subqueries (compile-join-filter-subqueries quote-marks joins)}))

(defn clean-up-all-conditions
  "Receives all-conditions produced by process-conditions. Only keeps
  non-empty conditions."
  [all-conditions]
  (let [all-conditions (remove nil? all-conditions)]
    (case (count all-conditions)
      0 nil
      1 (first all-conditions)
      (vec all-conditions))))

(defn process-pagination
  "Processes :offset :limit and :order-by if provided in current
  om.next query params."
  [{::keys [sql-schema] :as env}]
  {:pre [(s/valid? (s/keys :req [::column-names]) sql-schema)]
   :post [#(s/valid? (s/keys :req-un [::offset ::limit ::order-by]) %)]}
  (let [{::keys [column-names]} sql-schema]
    {:offset
     (when-let [offset (get-in env [:ast :params :offset])]
       (when (integer? offset)
         offset))
     :limit
     (when-let [limit (get-in env [:ast :params :limit])]
       (when (integer? limit)
         limit))
     :order-by
     (when-let [order-by (get-in env [:ast :params :order-by])]
       (pagination/->order-by-string column-names order-by))}))

(defn process-conditions
  "Combines all conditions to produce the final WHERE
  statement. Returns a vector (which implies an AND) of:

  - ident-condition: eg [:person/by-id 1] will result `WHERE person.id
  = 1`

  - join-condition: to filter the joined table given the attribute of
  the entity from upper level.

  - extra-condition: extra constraints for an ident or a join defined
  in schema.

  - supplied-condition: ad-hoc condition supplied in om.next
  query (often by client apps)"
  [{::keys [sql-schema] :as env}]
  (let [{::keys [ident-conditions]}
        sql-schema
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
  [{::keys [sql-schema] :as env} columns-to-query]
  (let [{::keys [column-names join-filter-subqueries]} sql-schema

        all-conditions (clean-up-all-conditions (process-conditions env))]
    (when all-conditions
      (->> all-conditions
        (expressions/parameterize {:column-names           column-names
                               :join-filter-subqueries join-filter-subqueries})
        ((juxt :raw-string :params))))))

(defn process-selection
  [{::keys [sql-schema] :as env} columns-to-query]
  (let [{::keys [column-names clojuric-names]} sql-schema

        target-column (env/target-column env)]
    (concat
      (mapv (fn [k]
              (let [column-name   (get column-names k)
                    clojuric-name (get clojuric-names k)]
                (if (string? column-name)
                  {:raw-string (str column-name " AS " clojuric-name)
                   :params     []}
                  ;; not string? it must be a pseudo-column
                  (let [form (s/conform ::expressions/expression column-name)]
                    (expressions/inline-params
                      {:raw-string (str "(?) AS " clojuric-name)
                       :params     [(expressions/process-expression {:column-names column-names} form)]})))))
        columns-to-query)
      (when target-column
        (let [form (s/conform ::expressions/expression (env/source-column-value env))]
          [(expressions/inline-params
             {:raw-string (str "? AS " (get clojuric-names target-column))
              :params     [(expressions/process-expression {:column-names column-names} form)]})])))))

(defn parameterize-all-selection
  [env columns-to-query]
  (let [column-names (-> env ::sql-schema ::column-names)
        xs (process-selection env columns-to-query)]
    (->> {:raw-string (->> (repeat (count xs) \?)
                        (clojure.string/join ", "))
          :params     xs}
      (expressions/inline-params)
      ((juxt :raw-string :params)))))

(defn process-all-params
  "Replaces any keyword found in all-params with their corresponding
  column-name"
  [env all-params]
  (let [column-names (-> env ::sql-schema ::column-names)]
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
  [{::keys [sql-schema] :as env}]
  {:pre [(s/valid? (s/keys :req [::column-names
                                 ::clojuric-names
                                 ::quote-marks]
                     :opt [::join-statements
                           ::target-tables
                           ::target-columns])
           sql-schema)]}
  (let [{::keys [quote-marks]}                   sql-schema
        k                                        (env/dispatch-key env)
        {:keys [join-children columns-to-query]} (process-children env)
        columns-to-query                         (if-let [target-column (env/target-column env)]
                                                   (conj columns-to-query target-column)
                                                   columns-to-query)
        [selection select-params]                (parameterize-all-selection env columns-to-query)
        [where-conditions where-params]          (parameterize-all-conditions env columns-to-query)
        {:keys [offset limit order-by]}          (process-pagination env)]
    {:query-string-input {:target-table     (env/target-table env)
                          :join-statement   (env/join-statement env)
                          :selection        selection
                          :quote-marks      quote-marks
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

  - sql-schema: output of compile-schema

  - sql-db: a database instance

  - run-query: a function that run an SQL query (optionally with
  params) against the given sql-db. Shares the same signature with
  clojure.java.jdbc/query."
  [{::keys [sql-schema sql-db run-query] :as env}]
  (let [{::keys [ident-keywords
                 batch-query
                 target-tables
                 target-columns
                 source-columns
                 join-statements
                 cardinality]} sql-schema
        k                           (env/dispatch-key env)]
    (if (contains? target-tables k)
      ;; this is an ident or a join, let's go for data
      (let [{:keys [query-string-input query-params join-children]}
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
                              (assoc-in [(get env ::p/entity-key) source-column]
                                (get e source-column)))))

                        query-strings (map #(->query-string (:query-string-input %)) query-string-inputs)
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
            (if one?
              #(p/join (first %2) %1)
              #(p/join-seq %1 %2))]
        (if (seq entities-with-join-children-data)
          (do-join env entities-with-join-children-data)
          (when-not one?
            [])))

      ::p/continue)))

(defn async-pull-entities
  "An async Pathom plugin that pulls entities from SQL database and
  puts relevent data to ::p/entity ready for p/map-reader plugin.

  The env given to the Pathom parser must contains:
  - sql-schema: output of compile-schema
  - sql-db: a database instance
  - run-query: a function that run an SQL query (optionally with
  params) against the given sql-db. Shares the same input with
  clojure.java.jdbc/query. Returns query result in a channel."
  [{::keys [sql-schema sql-db run-query] :as env}]
  (let [{::keys [ident-keywords
                 batch-query
                 target-tables
                 target-columns
                 source-columns
                 join-statements
                 cardinality]} sql-schema
        k                      (env/dispatch-key env)]
    (if (contains? target-tables k)
      ;; this is an ident or a join, let's go for data
      (let [{:keys [query-string-input query-params join-children]}
            (process-query env)

            query-string
            (when (contains? ident-keywords k)
              (->query-string query-string-input))

            one?
            (= :one (get cardinality k))

            do-join
            (if one?
              #(p/join (first %2) %1)
              #(p/join-seq %1 %2))

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
                                (assoc-in [(get env ::p/entity-key) source-column]
                                  (get e source-column)))))

                          query-strings (map #(->query-string (:query-string-input %)) query-string-inputs)
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
                (>! result-chan
                  (if (seq entities-with-join-children-data)
                    (<! (do-join env entities-with-join-children-data))
                    (if one?
                      {}
                      [])))))
          result-chan))

      ::p/continue)))
