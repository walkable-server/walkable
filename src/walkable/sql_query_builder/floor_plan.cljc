(ns walkable.sql-query-builder.floor-plan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.core :as p]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.helper :as helper]
            [walkable.sql-query-builder.pagination :as pagination]))

(def floor-plan-ns (namespace ::this))

(defn source-column-symbol [i]
  (symbol floor-plan-ns (str "source-column-" i)))

(defn ->join-filter [i target-column]
  [:= (source-column-symbol i) target-column])

(comment
  (= (->join-filter 0 :foo/bar)
     [:= `source-column-0 :foo/bar]))

(defn source-column-variable-values
  [values]
  {:variable-values
   (->> values
        (into {}
              (map-indexed (fn [i v]
                             [(source-column-symbol i)
                              (expressions/compile-to-string {} v)]))))})

(defn expand-join-first-hop
  [m]
  (let [source-columns (vec (keys m))
        target-columns (vec (vals m))
        filters (map ->join-filter (range) target-columns)]
    {:source-columns source-columns
     :source-column-values (apply juxt source-columns)
     :target-columns target-columns
     :target-column-values (apply juxt target-columns)
     :join-filter (if (< 1 (count filters))
                    (into [:and] filters)
                    (first filters))}))

(defn join-first-hop
  [_registry {:keys [:join-path] :as attr}]
  (merge (expand-join-first-hop (first join-path))
         attr))

(comment
  (let [{f :source-column-values} (expand-join-first-hop {:a/x :b/y})]
    (mapv f [{:a/x 1}
             {:a/x 2}
             {:a/x 5}]))
  (let [{f :source-column-values} (expand-join-first-hop {:a/x :b/y})]
    (mapv f [{:a/x 1}
             {:a/x 2}
             {:a/x 5}]))
  (expand-join-first-hop {:a/x :b/y})
  (expand-join-first-hop {:a/x :b/y :a/m :b/n})
  (expand-join-first-hop {:a/x :b/y :a/m :b/n :a/t :b/z}))

(def prop->func
  {:table-name emitter/table-name
   :column-name emitter/column-name
   :clojuric-name emitter/clojuric-name})

(defn true-column? [attr]
  (= :true-column (:type attr)))

(defn known-true-columns [attributes]
  (->> attributes
    (filterv true-column?)
    (helper/build-index :key)))

(defn keyword-prop
  [{:keys [:emitter :attributes]} k prop]
  (or (get-in (known-true-columns attributes) [k prop])
    (let [f (prop->func prop)]
      (f emitter k))))

(defn second-hop-join-statement*
  [registry {:keys [:join-path]}]
  ;; TODO: use spec
  (when (= 2 (count join-path))
    (let [pairs (second join-path)
          table (keyword-prop registry (second (first pairs)) :table-name)
          conditions (for [pair pairs
                           :let [[k v] (mapv #(keyword-prop registry % :column-name) pair)]]
                       (str k " = " v))]
      (when (not-empty conditions)
        (str " JOIN " table " ON "
             (clojure.string/join " AND " conditions))))))

(defn conditionally-update* [x pred f]
  (if (pred x)
    (f x)
    x))

(defn conditionally-update
  [xs pred f]
  (mapv #(conditionally-update* % pred f) xs))

(defn derive-missing-key [m k f]
  (if (contains? m k)
    m
    (assoc m k (f))))

(defn second-hop-join-statement
  "Generate JOIN statement if not provided. Only for \"two-hop\" join paths."
  [registry item]
  (derive-missing-key item :join-statement #(second-hop-join-statement* registry item)))

(defn join-target-table
  [registry {:keys [:target-columns] :as item}]
  ;; TODO: check if target-columns are in the same table
  (derive-missing-key item :target-table #(keyword-prop registry (first target-columns) :table-name)))

(defn column-list
  [registry columns]
  (let [column-names (map #(keyword-prop registry % :column-name) columns)]
    (clojure.string/join ", " column-names)))

(defn row-constructor
  [registry columns]
  (if (= 1 (count columns))
    (keyword-prop registry (first columns) :column-name)
    (str "("
         (column-list registry columns)
         ")")))

(defn join-filter-subquery*
  [registry {:keys [:join-statement :source-columns :target-columns :target-table]}]
  (str (row-constructor registry source-columns)
       " IN ("
       (emitter/->query-string
        {:selection (column-list registry target-columns)
         :target-table target-table
         :join-statement join-statement})
       " WHERE ?)"))

(defn join-filter-subquery
  [registry {:keys [:aggregate] :as item}]
  (if aggregate
    item
    (derive-missing-key item :join-filter-subquery #(join-filter-subquery* registry item))))

(defn compile-join
  [registry attribute]
  (->> attribute
    (join-first-hop registry)
    (join-target-table registry)
    (second-hop-join-statement registry)
    (join-filter-subquery registry)))

(defn compile-joins
  [registry]
  (update registry :attributes
    conditionally-update
    (fn [attr] (= :join (:type attr)))
    (fn [attr] (compile-join registry attr))))

(defn replace-join-with-source-columns-in-outputs
  [{:keys [:attributes] :as registry}]
  (let [join->source-columns
        (->> attributes
          (filter #(= :join (:type %)))
          (helper/build-index-of :source-columns))

        plain-join-keys
        (->> attributes
          (filter #(and (= :join (:type %)) (not (:aggregate %))))
          (map :key)
          set)

        join-key-sets
        (set (keys join->source-columns))]
    (update registry :attributes
      conditionally-update
      :output
      (fn [{:keys [:output] :as attr}]
        (let [joins-in-output
              (filter join-key-sets output)

              source-column-is-ident?
              (if (:primary-key attr)
                #{(:key attr)}
                (constantly false))

              source-columns
              (->> (mapcat join->source-columns joins-in-output)
                (remove source-column-is-ident?)
                set)

              new-output
              (set/union source-columns
                (set (remove plain-join-keys output)))]
          (assoc attr :output (vec new-output)))))))

(defn check-duplicate-keys [attrs]
  ;; TODO: implement with loop/recur to
  ;; tell which key is duplicated
  (when-not (apply distinct? (map :key attrs))
    (throw (ex-info "Duplicate keys" {}))))

(defn compile-formula
  [registry item]
  ;; TODO: catch exception when joins are used inside formulas
  (derive-missing-key item :compiled-formula
    #(expressions/compile-to-string registry (:formula item))))

(defn has-formula?
  [attr]
  (or (:aggregate attr)
    (= :pseudo-column (:type attr))
    (:primary-key attr)))

(defn compile-formulas
  [registry]
  (update registry :attributes
    conditionally-update
    has-formula?
    #(compile-formula registry %)))

(defn keyset [coll]
  (->> coll
    (map :key)
    (into #{})))

(defn find-aggregators
  [attributes]
  (->> attributes
    (filter :aggregate)
    (keyset)))

(defn find-pseudo-columns
  [attributes]
  (->> attributes
    (filter #(= :pseudo-column (:type %)))
    (keyset)))

(defn collect-dependencies
  [attributes]
  ;; TODO: use clojure.core transducers
  (reduce (fn [acc attr]
            (let [k (:key attr)
                  dependencies (->> attr :compiled-formula :params
                                 (filter expressions/atomic-variable?)
                                 (map :name)
                                 distinct
                                 (map #(vector k %)))]
              (concat acc dependencies)))
    ()
    (filter :compiled-formula attributes)))

(defn check-dependency-on-aggregators
  [aggregators dependencies]
  (when-let [[dependent dependency]
             (some (fn [[dependent dependency]]
                     (and (contains? aggregators dependency)
                       ;; returned by some:
                       [dependent dependency]))
               dependencies)]
    (throw (ex-info (str  "Please check `" dependent "`'s formula which contains an aggregator (`"
                      dependency "`)")
             {:formula dependent
              :dependency dependency}))))

(defn collect-independent-pseudo-columns
  [pseudo-columns dependencies]
  (let [deps
        (filter (fn [[x y]]
                  (and (contains? pseudo-columns x)
                    (contains? pseudo-columns y)))
          dependencies)
        Xs (into #{} (map first deps))
        Ys (into #{} (map second deps))]
    (set/difference Ys Xs)))

(defn get-prop [attrs k prop]
  (let [x (some #(and (= k (:key %)) %) attrs)]
    (get x prop)))

(defn direct-dependents [dependencies independents]
  (->> dependencies
    (filter (fn [[_ y]] (contains? independents y)))
    (map first)
    (into #{})))

(defn get-compiled-formula [attrs ks]
  (->> (for [k ks]
         [k (get-prop attrs k :compiled-formula)])
    (into {})))

(defn expand-nested-pseudo-columns*
  [attrs]
  (loop [attrs attrs]
    (let [dependencies (collect-dependencies attrs)
          pseudo-columns (find-pseudo-columns attrs)
          independents (collect-independent-pseudo-columns pseudo-columns dependencies)]
      (if (empty? independents)
        attrs
        (let [realized-formulas (get-compiled-formula attrs independents)]
          (recur (conditionally-update attrs
                   (fn [attr] (contains? (direct-dependents dependencies independents)
                                (:key attr)))
                   (fn [attr]
                     (update attr :compiled-formula
                       (fn [cf]
                         (expressions/substitute-atomic-variables
                           {:variable-values realized-formulas}
                           cf)))))))))))

(defn expand-nested-pseudo-columns
  [registry]
  (update registry :attributes expand-nested-pseudo-columns*))

(defn expand-pseudo-columns-in-aggregators*
  [attrs]
  (let [compiled-pseudo-columns (get-compiled-formula attrs (find-pseudo-columns attrs))]
    (conditionally-update attrs
      #(:aggregate %)
      #(update % :compiled-formula
         (fn [cf]
           (expressions/substitute-atomic-variables
             {:variable-values compiled-pseudo-columns}
             cf))))))

(defn expand-pseudo-columns-in-aggregators
  [registry]
  (update registry :attributes expand-pseudo-columns-in-aggregators*))

(defn prefix-having [compiled-having]
  (expressions/inline-params {}
    {:raw-string " HAVING (?)"
     :params     [compiled-having]}))

(defn join-filter-subqueries
  [{:keys [:attributes] :as registry}]
  (let [jfs (->> attributes
              (filter #(and (= :join (:type %)) (not (:aggregate %))))
              (helper/build-index-of :join-filter-subquery))]
    (assoc registry :join-filter-subqueries jfs)))

(defn compile-filters
  ;; TODO: just extracted join-filter-subqueries above, not checked calling of compile-filters yet
  [{:keys [:attributes] :as registry}]
  (let [compiled-pseudo-columns (get-compiled-formula attributes (find-pseudo-columns attributes))]
    (assoc registry :attributes
      (-> attributes
        (conditionally-update
          #(and (:filter %) (not (:compiled-filter %)))
          (fn [{condition :filter :as attr}]
            (->> condition
              (expressions/compile-to-string registry)
              (expressions/substitute-atomic-variables
                {:variable-values compiled-pseudo-columns})
              (assoc attr :compiled-filter))))
        (conditionally-update
          #(and (:ident-filter %) (not (:compiled-ident-filter %)))
          (fn [{condition :ident-filter :as attr}]
            (->> condition
              (expressions/compile-to-string registry)
              (expressions/substitute-atomic-variables
                {:variable-values compiled-pseudo-columns})
              (assoc attr :compiled-ident-filter))))
        (conditionally-update
          #(and (:join-filter %) (not (:compiled-join-filter %)))
          (fn [{condition :join-filter :as attr}]
            (->> condition
              (expressions/compile-to-string registry)
              (expressions/substitute-atomic-variables
                {:variable-values compiled-pseudo-columns})
              (assoc attr :compiled-join-filter))))
        (conditionally-update
          #(and (:having %) (not (:compiled-having %)))
          (fn [{condition :having :as attr}]
            (->> condition
              (expressions/compile-to-string registry)
              (expressions/substitute-atomic-variables
                {:variable-values compiled-pseudo-columns})
              (prefix-having)
              (assoc attr :compiled-having))))))))

(defn compile-all-filters
  [{:keys [:attributes] :as registry}]
  (assoc registry :attributes
    (-> attributes
      (conditionally-update
        :primary-key
        (fn [{:keys [:compiled-filter :compiled-ident-filter] :as attr}]
          (assoc attr :all-filters
            (let [without-supplied-filter
                  (expressions/concat-with-and
                    (into [] (remove nil?) [compiled-filter compiled-ident-filter]))]
              (fn [_supplied-filter]
                without-supplied-filter)))))
      (conditionally-update
        #(#{:join :root} (:type %))
        (fn [{:keys [:compiled-filter :compiled-join-filter] :as attr}]
          (let [without-supplied-filter
                (expressions/concat-with-and
                  (into [] (remove nil?) [compiled-filter compiled-join-filter]))
                with-supplied-filter
                (expressions/concat-with-and
                  (into [] (remove nil?) [compiled-filter
                                          compiled-join-filter
                                          (expressions/compile-to-string {} (expressions/av `supplied-filter))]))]
            (assoc attr :all-filters
              (fn [supplied-filter]
                (if supplied-filter
                  (expressions/substitute-atomic-variables
                    {:variable-values {`supplied-filter supplied-filter}}
                    with-supplied-filter)
                  without-supplied-filter)))))))))

(defn compile-variable
  [{k :key :keys [cached? compute] :as attr}]
  (let [f (if cached?
            (fn [env _computed-graphs]
              (p/cached env [:walkable/variable-getter k]
                (compute env)))
            (fn [env _computed-graphs]
              (compute env)))]
    (assoc attr :compiled-variable-getter f)))

(defn compile-variables*
  [attrs]
  (conditionally-update attrs
    #(= :variable (:type %))
    compile-variable))

(defn compile-variables
  [registry]
  (update registry :attributes compile-variables*))

(defn compile-pagination-fallback
  [{:keys [emitter clojuric-names]} attr]
  (assoc attr :compiled-pagination-fallbacks
    {:offset-fallback (pagination/offset-fallback emitter
                        {:default (:default-offset attr)
                         :validate (:validate-offset attr)
                         :throw? (= :error (:on-invalid-offset attr))})
     :limit-fallback (pagination/limit-fallback emitter
                       {:default (:default-limit attr)
                        :validate (:validate-limit attr)
                        :throw? (= :error (:on-invalid-limit attr))})
     :order-by-fallback (pagination/order-by-fallback emitter clojuric-names
                          {:default (:default-order-by attr)
                           :validate (:validate-order-by attr)
                           :throw? (= :error (:on-invalid-order-by attr))})}))

(defn compile-pagination-fallbacks
  [registry]
  (update registry :attributes
    conditionally-update
    #(or (and (#{:root :join} (:type %)) (not (:aggregate %)))
       (= pagination/default-fallbacks (:key %)))
    #(compile-pagination-fallback registry %)))

(defn ident-filter
  [_registry item]
  (derive-missing-key item :ident-filter (constantly [:= (:key item) (expressions/av `ident-value)])))

(defn derive-ident-cardinality
  [registry]
  (update registry :attributes
    conditionally-update
    :primary-key
    (fn [attr] (assoc attr :cardinality :one))))

(defn derive-ident-filters
  [registry]
  (update registry :attributes
    conditionally-update
    :primary-key
    (fn [attr] (ident-filter registry attr))))

(def derive-join-filters identity)

(defn collect-compiled-formulas
  [{:keys [:attributes] :as registry}]
  (assoc registry :compiled-formulas
    (helper/build-index-of :compiled-formula (filter #(#{:true-column :pseudo-column} (:type %)) attributes))))

(defn compile-group-by*
  [compiled-formulas group-by-keys]
  (->> group-by-keys
    (map compiled-formulas)
    (map :raw-string)
    (string/join ", ")
    (str " GROUP BY ")))

(defn compile-group-by
  [{:keys [:compiled-formulas] :as registry}]
  (update registry :attributes
    conditionally-update
    (fn [attr] (and (= :root (:type attr)) (:group-by attr)))
    (fn [attr] (derive-missing-key attr :compiled-group-by
                 #(compile-group-by* compiled-formulas (:group-by attr))))))

(defn return-one [entities]
  (if (not-empty entities)
    (first entities)
    {}))

(defn return-many [entities]
  (if (not-empty entities)
    entities
    []))

(defn compile-return-function
  [{:keys [:cardinality :aggregate] k :key :as attr}]
  (let [f (if aggregate
            #(get (first %) k)
            (if (= :one cardinality)
              return-one
              return-many))]
    (assoc attr :return f)))

(defn compile-return-functions
  [registry]
  (update registry :attributes
    conditionally-update
    #(or (#{:root :join} (:type %)) (:primary-key %))
    compile-return-function))

(defn group-registry
  [flat-registry]
  (let [emitter (or (some #(and (= `emitter (:key %)) (emitter/build-emitter %)) flat-registry)
                  emitter/default-emitter)
        operator-set (or (some #(and (= `operator-set (:key %)) (expressions/build-operator-set %)) flat-registry)
                       expressions/common-operators)]
    {:emitter emitter
     :batch-query (emitter/emitter->batch-query emitter)
     :operators (helper/build-index :key operator-set)
     :attributes flat-registry}))

(defn collect-outputs [attrs]
  (into #{} (mapcat :output) attrs))

;; TODO: replace with source-columns, target-columns (both first-hop and second-hop)
(defn collect-join-paths [attrs]
  (into #{}
        (apply concat
               (for [m (mapcat :join-path attrs)
                     [k v] m]
                 [k v]))))

(defn collect-under-key [k]
  (comp (map k) (remove nil?) (mapcat :params) (filter expressions/atomic-variable?) (map :name)))

(defn collect-atomic-variables*
  [attrs]
  (set/union
    (into #{} (collect-under-key :compiled-formula)
      attrs)
    (into #{} (collect-under-key :compiled-filter)
      attrs)
    (into #{} (collect-under-key :compiled-ident-filter)
      attrs)
    (into #{} (collect-under-key :compiled-join-filter)
      attrs)
    (into #{} (collect-under-key :compiled-having)
      attrs)))

(defn group-atomic-variables [coll]
  (set/rename-keys  (group-by symbol? coll) {true :found-variables false :found-columns}))

(defn collect-atomic-variables
  [attrs]
  (group-atomic-variables (collect-atomic-variables* attrs)))

(defn variables-and-true-columns
  [{:keys [:attributes] :as registry}]
  (let [{:keys [:found-variables :found-columns]} (collect-atomic-variables attributes)
        non-true-columns (into #{} (comp (filter #(#{:root :join :pseudo-column} (:type %))) (map :key)) attributes)
        all-columns (set (set/union found-columns
                                    (collect-outputs attributes)
                                    (collect-join-paths attributes)))
        true-columns (set/difference all-columns non-true-columns)]
    (merge registry
           {:true-columns true-columns
            :found-variables found-variables})))

(defn fill-true-column-attributes
  [{:keys [:attributes :true-columns] :as registry}]
  (let [exists (set (keys (known-true-columns attributes)))
        new (set/difference true-columns exists)]
    (update registry :attributes into (mapv (fn [k] {:key k :type :true-column}) new))))

(defn selectable? [attr]
  (or (#{:true-column :pseudo-column} (:type attr))
    (:aggregate attr)))

(defn compile-clojuric-names
  [{:keys [:emitter] :as registry}]
  (update registry :attributes
    conditionally-update
    selectable?
    #(assoc % :clojuric-name (emitter/clojuric-name emitter (:key %)))))

(defn collect-clojuric-names
  [{:keys [:attributes] :as registry}]
  (assoc registry :clojuric-names
         (helper/build-index-of :clojuric-name (filter :clojuric-name attributes))))

(defn compile-true-columns
  [{:keys [:emitter] :as registry}]
  (update registry :attributes
          conditionally-update
          #(#{:true-column} (:type %))
          ;; TODO: take into account existing prop :table, etc
          #(let [inline-form (emitter/column-name emitter (:key %))]
             (assoc % :compiled-formula {:raw-string inline-form :params []}))))

(defn inline-into
  [k inline-forms]
  (fn [attr]
    (update attr k
      #(expressions/substitute-atomic-variables
         {:variable-values inline-forms} %))))

(defn inline-true-columns
  [{:keys [:attributes] :as registry}]
  (let [inline-forms (->> attributes
                       (filterv true-column?)
                       (helper/build-index-of :compiled-formula))]
    (assoc registry :attributes
      (-> attributes
        (conditionally-update
          #(:compiled-filter %)
          (inline-into :compiled-filter inline-forms))
        (conditionally-update
          #(:compiled-ident-filter %)
          (inline-into :compiled-ident-filter inline-forms))
        (conditionally-update
          #(:compiled-join-filter %)
          (inline-into :compiled-join-filter inline-forms))
        (conditionally-update
          #(:compiled-having %)
          (inline-into :compiled-having inline-forms))
        (conditionally-update
          #(and (:compiled-formula %) (not (= :true-column (:type %))))
          (inline-into :compiled-formula inline-forms))))))

(defn compile-selection
  [registry]
  (update registry :attributes
    conditionally-update
    #(or (#{:true-column :pseudo-column} (:type %)) (:aggregate %))
    (fn [{:keys [:compiled-formula :clojuric-name] :as attr}]
      (let [selection (expressions/selection compiled-formula clojuric-name)]
        (assoc attr :compiled-selection selection)))))

(defn compile-join-aggregator-selection
  [{:keys [:emitter] :as registry}]
  (update registry :attributes
    conditionally-update
    #(and (= :join (:type %)) (:aggregate %))
    (fn [{:keys [:compiled-formula :clojuric-name] :as attr}]
      (let [{:keys [:target-columns]} attr

            aggregator-selection
            (expressions/selection compiled-formula clojuric-name)

            source-column-selection
            (->> target-columns
                 (map-indexed #(expressions/selection
                                {:raw-string "?"
                                 :params [(expressions/av (source-column-symbol %1))]}
                                (emitter/clojuric-name emitter %2))))]
        (assoc attr :compiled-join-aggregator-selection
          (expressions/concatenate #(clojure.string/join ", " %)
                                   (into [aggregator-selection] source-column-selection)))))))

(defn compile-join-selection
  [{:keys [:emitter] :as registry}]
  (update registry :attributes
          conditionally-update
          #(= :join (:type %))
          (fn [{:keys [:target-columns] :as attr}]
            (->> target-columns
                 (map-indexed #(expressions/selection
                                {:raw-string "?"
                                 :params [(expressions/av (source-column-symbol %1))]}
                                (emitter/clojuric-name emitter %2)))
                 (expressions/concatenate #(clojure.string/join ", " %))
                 (assoc attr :selection)))))

(defn compile-traverse-scheme
  [{attr-type :type :as attr}]
  (cond
    (= :root attr-type)
    (assoc attr :traverse-scheme :roots)

    (= :join attr-type)
    (assoc attr :traverse-scheme :joins)

    (#{:true-column :pseudo-column} attr-type)
    (assoc attr :traverse-scheme :columns)

    :else
    attr))

(defn compile-traverse-schemes [registry]
  (update registry :attributes
    #(mapv compile-traverse-scheme %)))

(defn compile-join-sub-entities
  [registry]
  (update registry :attributes
    conditionally-update
    #(= :join (:type %))
    (fn [{:keys [:return :target-column-values :source-column-values] :as attr}]
      (assoc attr :merge-sub-entities
        (fn ->merge-sub-entities [result-key]
          (fn merge-sub-entities [entities sub-entities]
            (if (empty? sub-entities)
              entities
              (let [groups (group-by target-column-values sub-entities)]
                (->> entities
                     (mapv (fn [entity]
                             (let [scv (source-column-values entity)]
                               (assoc entity result-key (return (get groups scv)))))))))))))))

(defn compile-root-sub-entities
  [registry]
  (update registry :attributes
    conditionally-update
    #(or (= :root (:type %)) (:primary-key %))
    (fn [{:keys [:return] :as attr}]
      (assoc attr :merge-sub-entities
        (fn ->merge-sub-entities [result-key]
          (fn merge-sub-entities [entities sub-entities]
            (if (empty? sub-entities)
              entities
              (assoc entities result-key (return sub-entities)))))))))

(defn compile-query-multiplier
  [{:keys [batch-query] :as registry}]
  (update registry :attributes
          conditionally-update
          #(= :join (:type %))
          (fn [{:keys [:source-column-values] :as attr}]
            (assoc attr :query-multiplier
                   (fn ->query-multiplier [individual-query-template]
                     (let [xform (comp (map source-column-values)
                                       (remove #(every? nil? %))
                                       (map #(-> (expressions/substitute-atomic-variables
                                                  (source-column-variable-values %)
                                                  individual-query-template)
                                                 ;; attach source-column-value as meta data
                                                 #_(with-meta {:source-column-value %}))))]
                       (fn query-multiplier* [_env entities]
                         (->> entities
                              ;; TODO: substitue-atomic-variables per entity
                              (into [] (comp xform))
                              batch-query))))))))

(defn ident-table*
  [registry item]
  (derive-missing-key item :table #(keyword-prop registry (:key item) :table-name)))

(defn derive-ident-table
  [registry]
  (update registry :attributes
    conditionally-update
    :primary-key
    (fn [attr] (ident-table* registry attr))))

(defn inputs-outputs
  [{:keys [:attributes]}]
  (let [plain-roots
        (->> attributes
          (filter #(and (= :root (:type %)) (not (:aggregate %))))
          (mapv (fn [{k :key :keys [:output]}]
                  {::pc/input #{}
                   ::pc/output [{k output}]})))

        root-aggregators
        (->> attributes
          (filter #(and (= :root (:type %)) (:aggregate %)))
          (mapv (fn [{k :key}]
                  {::pc/input #{}
                   ::pc/output [k]})))

        plain-joins
        (->> attributes
          (filter #(and (= :join (:type %)) (not (:aggregate %))))
          (mapv (fn [{k :key :keys [:output :source-columns]}]
                  {::pc/input (into #{} source-columns)
                   ::pc/output [{k output}]})))

        join-aggregators
        (->> attributes
          (filter #(and (= :join (:type %)) (:aggregate %)))
          (mapv (fn [{k :key :keys [:source-columns]}]
                  {::pc/input (into #{} source-columns)
                   ::pc/output [k]})))

        idents (->> attributes
                 (filter :primary-key)
                 (mapv (fn [{k :key :keys [:output]}]
                         {::pc/input #{k}
                          ::pc/output output})))]
    (concat plain-roots root-aggregators plain-joins join-aggregators idents)))

(defn floor-plan [{:keys [:attributes]}]
  ;; build a compact version suitable for expressions/compile-to-string
  (merge
   {:target-table (merge (helper/build-index-of :target-table (filter #(= :join (:type %)) attributes))
                         (helper/build-index-of :table (filter #(or (= :root (:type %)) (:primary-key %)) attributes)))}
   {:target-columns (helper/build-index-of :target-columns (filter #(= :join (:type %)) attributes))}
   {:target-column-values (helper/build-index-of :target-column-values (filter #(= :join (:type %)) attributes))}
   {:source-columns (helper/build-index-of :source-columns (filter #(= :join (:type %)) attributes))}
   {:source-column-values (helper/build-index-of :source-column-values (filter #(= :join (:type %)) attributes))}
   {:merge-sub-entities (helper/build-index-of :merge-sub-entities (filter #(or (#{:root :join} (:type %)) (:primary-key %)) attributes))}
   {:query-multiplier (helper/build-index-of :query-multiplier (filter :query-multiplier attributes))}
   {:join-statement (helper/build-index-of :join-statement (filter :join-statement attributes))}
   {:compiled-join-selection (helper/build-index-of :compiled-join-selection (filter :compiled-join-selection attributes))}
   {:compiled-join-aggregator-selection (helper/build-index-of :compiled-join-aggregator-selection (filter :compiled-join-aggregator-selection attributes))}
   {:keyword-type (helper/build-index-of :traverse-scheme (filter #(#{:root :join :true-column :pseudo-column} (:type %)) attributes))}
   {:compiled-pagination-fallbacks (helper/build-index-of :compiled-pagination-fallbacks (filter :compiled-pagination-fallbacks attributes))}
   {:return (helper/build-index-of :return (filter #(or (#{:root :join} (:type %)) (:primary-key %)) attributes))}
   {:aggregator-keywords (into #{} (comp (filter #(:aggregate %)) (map :key)) attributes)}
   {:compiled-variable-getter (helper/build-index-of :compiled-variable-getter (filter :compiled-variable-getter attributes))}
   {:all-filters (helper/build-index-of :all-filters (filter :all-filters attributes))}
   {:compiled-having (helper/build-index-of :compiled-having (filter :compiled-having attributes))}
   {:compiled-group-by (helper/build-index-of :compiled-group-by (filter :compiled-group-by attributes))}
   {:ident-keywords (into #{} (comp (filter #(:primary-key %)) (map :key)) attributes)}
   {:compiled-selection (helper/build-index-of :compiled-selection (filter :compiled-selection attributes))}))

(defn compact [registry]
  {:floor-plan (merge (select-keys registry [:emitter :operators :join-filter-subqueries :batch-query :compiled-formulas :clojuric-names])
                 (floor-plan registry))
   :inputs-outputs (inputs-outputs registry)})

(defn with-db-type
  [db-type registry]
  (concat registry
    [{:key `emitter
      :base db-type}
     {:key `operator-set
      :base db-type}]))

(defn compile-floor-plan*
  [flat-attributes]
  (let [registry (group-registry flat-attributes)]
    (->> registry
      (compile-formulas)
      (expand-nested-pseudo-columns)
      (expand-pseudo-columns-in-aggregators)
      (compile-joins)
      (replace-join-with-source-columns-in-outputs)
      (derive-ident-table)
      (join-filter-subqueries)
      (derive-ident-filters)
      (derive-join-filters)
      (derive-ident-cardinality)
      (compile-filters)
      (compile-variables)
      (compile-return-functions)
      (variables-and-true-columns)
      (fill-true-column-attributes)
      (compile-true-columns)
      (compile-clojuric-names)
      (collect-clojuric-names)
      (inline-true-columns)
      (compile-pagination-fallbacks)
      (compile-selection)
      (compile-join-selection)
      (compile-join-aggregator-selection)
      (compile-traverse-schemes)
      (compile-join-sub-entities)
      (compile-root-sub-entities)
      (compile-query-multiplier)
      (compile-all-filters)
      (collect-compiled-formulas)
      (compile-group-by))))

(defn compile-floor-plan
  [flat-attributes]
  (->> flat-attributes
    (compile-floor-plan*)
    (compact)))
