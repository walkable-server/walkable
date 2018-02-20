(ns walkable.sql-query-builder
  (:require [walkable.sql-query-builder.filters :as filters]
            [com.wsscode.pathom.core :as p]))

(defn split-keyword
  "Splits a keyword into a tuple of table and column."
  [k]
  (->> ((juxt namespace name) k)
    (map #(-> % (clojure.string/replace #"-" "_")))))

(defn keyword->column-name [k]
  (->> (split-keyword k)
    (map #(str "`" % "`"))
    (clojure.string/join ".")))

(defn keyword->alias [k]
  (subs (str k) 1))

(defn ->column-names [ks]
  (zipmap ks
    (map keyword->column-name ks)))

(defn ->column-aliases [ks]
  (zipmap ks
    (map keyword->alias ks)))

(defn selection-with-aliases
  [columns-to-query column-names column-aliases]
  (->> columns-to-query
    (map #(str (get column-names %)
            " AS \""
            (get column-aliases %)
            "\""))
    (clojure.string/join ", ")))

(defn ->join-statement
  [[[table-1 column-1] [table-2 column-2]]]
  (str
    " JOIN `" table-2
    "` ON `"   table-1 "`.`" column-1 "` = `" table-2 "`.`" column-2 "`"))

(defn ->join-pairs
  [join-spec]
  (partition 2 (map split-keyword join-spec)))

(defn ->join-statements [join-pairs]
  (apply str (map ->join-statement join-pairs)))

(defn ->source-table [join-pairs]
  (doall (ffirst (first join-pairs))))

(defn ->query-string
  [{::keys [source-table join-statement
            columns-to-query column-names column-aliases
            where-conditions offset limit order-by]}]
  (str "SELECT " (selection-with-aliases columns-to-query column-names column-aliases)
    " FROM `" source-table "`"
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

(defn children->columns-to-query
  [{::keys [column-keywords required-columns source-table source-column]}
   all-child-keys]
  (let [child-column-keys
        (->> all-child-keys (filter #(contains? column-keywords %)) (into #{}))

        child-required-keys
        (->> all-child-keys (map #(get required-columns %)) (apply clojure.set/union))

        child-join-keys
        (filter #(contains? source-table %) all-child-keys)

        child-source-columns
        (->> child-join-keys (map #(get source-column %)) (into #{}))]
    (clojure.set/union
      child-column-keys
      child-required-keys
      child-source-columns)))

(defn ident->condition
  [env condition]
  (let [params         (-> env :ast :key rest)
        [operator key] condition]
    {key (cons operator params)}))

(defn conditional-idents->source-tables
  [idents]
  (reduce (fn [result [ident-key [_operator column-keyword]]]
            (assoc result ident-key
              (first (split-keyword column-keyword))))
    {} idents))

(defn joins->source-tables
  [joins]
  (reduce (fn [result [k join-spec]]
            (assoc result k
              (first (split-keyword (first join-spec)))))
    {} joins))

(defn joins->source-columns
  [joins]
  (reduce (fn [result [k join-spec]]
            (assoc result k
              (first join-spec)))
    {} joins))

(defn compile-ident-conditions
  [idents]
  (reduce (fn [result [k v]]
            (assoc result k
              (if (fn? v)
                v
                (fn [env] (ident->condition env v)))))
    {} idents))

(defn assoc-multi [m ks v]
  (if (sequential? ks)
    (merge m
      (reduce (fn [result k]
                (assoc result k v))
        {} ks))
    (assoc m ks v)))

(defn flatten-multi-keys [m]
  (reduce (fn [result [k v]]
            (assoc-multi result k v))
    {} m))

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
  (reduce (fn [result [k v]]
            (assoc result k
              (->join-statements (->join-pairs v))))
    {} joins))

(defn expand-reversed-joins [reversed-joins joins]
  (let [more (reduce (fn [result [backward forward]]
                       (assoc result backward
                         (reverse (get joins forward))))
               {} reversed-joins)]
    (merge joins more)))

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
  [{:keys [columns required-columns idents extra-conditions
           reversed-joins joins join-cardinality]}]
  (let [columns                                           (set columns)
        idents                                            (flatten-multi-keys idents)
        {:keys [unconditional-idents conditional-idents]} (separate-idents idents)
        extra-conditions                                  (flatten-multi-keys extra-conditions)
        joins                                             (->> (flatten-multi-keys joins)
                                                            (expand-reversed-joins reversed-joins))
        join-cardinality                                  (flatten-multi-keys join-cardinality)]
    #::{:column-keywords  columns
        :required-columns (expand-denpendencies required-columns)
        ;; SELECT ... FROM ?
        ;; derive from idents and joins
        :source-tables    (merge (conditional-idents->source-tables conditional-idents)
                            unconditional-idents
                            (joins->source-tables joins))
        :source-columns   (joins->source-columns joins)
        :join-cardinality join-cardinality
        :ident-conditions (compile-ident-conditions conditional-idents)
        :extra-conditions (compile-extra-conditions extra-conditions)
        :column-names     (->column-names columns)
        :column-aliases   (->column-aliases columns)
        :join-statements  (compile-join-statements joins)}))

(defn sql-pull
  [{::keys [sql-schema sql-db run-query] :as env}]
  (let [{::keys [column-keywords
                 column-names
                 column-aliases

                 required-columns

                 join-statements
                 join-cardinality
                 source-columns
                 source-tables

                 ident-conditions
                 extra-conditions]}
        sql-schema
        e (p/entity env)
        k (get-in env [:ast :dispatch-key])]
    (if (contains? source-tables k)
      (let [source-table
            (get source-tables k)

            source-column
            (get source-columns k)

            source-condition
            (when source-column
              {source-column [:= (get e source-column)]})

            all-child-keys
            (->> env :ast :children (map :dispatch-key))

            join-statement
            (get join-statements k)

            columns-to-query
            (children->columns-to-query sql-schema all-child-keys)

            ident-condition
            (when-let [condition (get ident-conditions k)]
              (condition env))

            extra-condition
            (when-let [condition (get extra-conditions k)]
              (condition env))

            supplied-condition
            (get-in env [:ast :params ::filters])

            all-conditions
            (remove nil? [ident-condition source-condition extra-condition supplied-condition])

            all-conditions
            (case (count all-conditions)
              0 nil
              1 (first all-conditions)
              all-conditions)

            [where-conditions sql-params]
            (when all-conditions
              (filters/parameterize {:key    nil
                                     :keymap column-names}
                all-conditions))

            offset
            (when-let [offset (get-in env [:ast :params ::offset])]
              (when (integer? offset)
                offset))

            limit
            (when-let [limit (get-in env [:ast :params ::limit])]
              (when (integer? limit)
                limit))

            order-by
            (when-let [order-by (get-in env [:ast :params ::order-by])]
              (filters/->order-by-string column-names order-by))

            sql-query
            (->query-string
              #::{:source-table     source-table
                  :join-statement   join-statement
                  :columns-to-query columns-to-query
                  :column-names     column-names
                  :column-aliases   column-aliases
                  :where-conditions where-conditions
                  :offset           offset
                  :limit            limit
                  :order-by         order-by})

            query-result
            (run-query sql-db
              (if sql-params
                (cons sql-query sql-params)
                sql-query))

            do-join
            (if (= :one (get join-cardinality k))
              #(p/join (first %2) %1)
              #(p/join-seq %1 %2))]
        (if (seq query-result)
          (do-join env query-result)
          []))

      ::p/continue)))
