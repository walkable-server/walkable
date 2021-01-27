(ns walkable.sql-query-builder.floor-plan
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [com.wsscode.pathom.core :as p]
            [clojure.set :as set]
            [clojure.string :as string]
            [weavejester.dependency :as dep]
            [clojure.spec.alpha :as s]
            [plumbing.graph :as graph]))

(defn check-circular-dependency!
  [graph]
  (try
    (reduce (fn [acc [x y]] (dep/depend acc y x))
      (dep/graph)
      graph)
    (catch Exception e
      (let [{:keys [node dependency] :as data} (ex-data e)]
        (throw (ex-info (str "Circular dependency between " node " and " dependency)
                 data))))))

(defn build-index [k coll]
  (into {} (for [o coll] [(get o k) o])))

(expressions/compile-to-string
  {:operators (build-index :key expressions/common-operators)}
  [:* [:+ :a/b 3] 6])

(expressions/compile-to-string
  {:operators (build-index :key expressions/common-operators)}
  [:cast :a/c :text])

;; steps
;; - compile joins
;; - derive ident filter
;; - compile formulas (aggretate + pseudo columns)
;; - expand nested pseudo-columns
;; - expand pseudo-columns in aggregator's formulas
;; - expand pseudo-columns in conditions
;; - compile group-by
;; - compile filter/ident-filter/having with compiled pseudo-columns and joins
;; - compile pagination

;; - throw errors on:
;;  + duplicate keys
;;  + circular dependencies
;;  + aggregators inside any formula (pseudo-columns or aggregator)
;;  + joins in pseudo-columns/aggregators
;;  + aggregators in pseudo-columns/aggregators

;; - collect all columns in compiled-formula, compiled-filter, compiled-join-filter, compiled-having
;; order-by, and output
;; - derive true-columns = all columns - known pseudo columns
;; - collect variables, check if all non-internal vars are defined
;; - inline true-columns in all places
;; - compile selection, join-selection and aggregator selection
;; - build the rest of floor-plan: cte
;; - compile cardinality
;; - compile return
;; - compile traverse-scheme
;; - warn if some true/pseudo columns not shown in any output
;; - generate connect graph for joins

(def my-attributes
  [{:key :people/list
    :type :root
    :table "person"
    :filter [:= :person/hidden false]
    :output [:person/id :person/name :person/yob]
    :offset/default 3
    :offset/on-invalid :return-empty
    :offset/validate #(<= 0 % 100)
    :limit/default 10
    :limit/on-invalid :return-error
    :limit/validate #(<= 1 % 20)
    :order-by/default [:farmer/number :asc]
    :order-by/on-invalid :return-default
    :order-by/validate #{:farmer/number :farmer/yob}}
   {:key :people/total
    :type :root
    :aggregate true
    :formula [:count-*]
    :table "person"
    :filter [:= :person/hidden false]}
   {:key :people/lied-age-sum
    :type :root
    :aggregate true
    :formula [:sum :person/lied-age]
    :table "person"
    :filter [:= :person/hidden false]}
   {:key :person/friend
    :type :join
    :join-path [:person/id :friendship/first-person :friendship/second-person :person/id]
    :filter [:= :person/hidden false]
    :output []}
   {:key :person/friends-count
    :type :join
    :aggregate true
    :formula [:count-*]
    :join-path [:person/id :friendship/first-person :friendship/second-person :person/id]
    :filter [:= :person/hidden false]
    :output []}
   {:key :person/age
    :type :pseudo-column
    :formula [:- 'current-year :person/yob]}
   {:key :person/lied-age
    :type :pseudo-column
    :formula [:- :person/age 10]}
   {:key :person/double-lied-age
    :type :pseudo-column
    :formula [:- :person/lied-age 10]}
   {:key 'current-year
    :type :variable
    :compute (fn [env] 2021)}
   {:key :person/id
    :type :primary-key
    :output []}])

(s/def ::without-join-table
  (s/coll-of ::expressions/namespaced-keyword
    :count 2))

(s/def ::with-join-table
  (s/coll-of ::expressions/namespaced-keyword
    :count 4))

(s/def ::join-path
  (s/or
   :without-join-table ::without-join-table
   :with-join-table    ::with-join-table))

(defn join-statement*
  [{:keys [emitter]} {:keys [join-path]}]
  (let [[tag] (s/conform ::join-path join-path)]
    (when (= :with-join-table tag)
      (let [[_source _join-source join-target target] join-path]
        (str
          ;; TODO: check if table-name/column-name for each attribute
          ;; specified elsewhere before calling emitter fuctions
          ;; => collect those and put in env's `:table-names` etc first
          " JOIN " (emitter/table-name emitter target)
          " ON " (emitter/column-name emitter join-target)
          ;; TODO: what about other operators than `=`?
          " = " (emitter/column-name emitter target))))))

(comment
  (join-statement*
    {:emitter emitter/default-emitter}
    {:key :person/friend
     :type :join
     :join-path [:person/id :friendship/first-person :friendship/second-person :person/id]})

  (join-statement*
    {:emitter emitter/default-emitter}
    {:key :person/house
     :type :join
     :join-path [:person/id :house/owner-id]}))

(defn conditionally-update* [x pred f]
  (if (pred x)
    (f x)
    x))

(comment
  (conditionally-update* {:x 2 :y 3} #(= 1 (:x %)) #(update % :x inc))
  (conditionally-update* {:x 2 :y 3}#(= 2 (:x %)) #(update % :x inc)))

(defn conditionally-update
  [xs pred f]
  (mapv #(conditionally-update* % pred f) xs))

(comment
  (conditionally-update [{:x 1 :y 1} {:x 2 :y 3}] #(= 2 (:x %)) #(update % :x inc))
  )

(defn derive-missing-key [m k f]
  (if (contains? m k)
    m
    (assoc m k (f))))

(comment
  (derive-missing-key {:x 3} :x (constantly 4))
  (derive-missing-key {:x nil} :x (constantly 4))
  (derive-missing-key {} :x (constantly 4)))

(defn join-statement
  "Generate JOIN statement if not provided. Only for \"two-hop\" join paths."
  [registry item]
  (derive-missing-key item :join-statement #(join-statement* registry item)))

(defn join-source-column
  [_registry {:keys [join-path] :as item}]
  (derive-missing-key item :source-column #(first join-path)))

(defn join-target-column
  [_registry {:keys [join-path] :as item}]
  (derive-missing-key item :target-column #(second join-path)))

(defn join-target-table
  [{:keys [emitter]} {:keys [target-column] :as item}]
  (derive-missing-key item :target-table #(emitter/table-name emitter target-column)))

(defn join-filter-subquery*
  [{:keys [emitter]} {:keys [join-statement target-table source-column target-column]}]
  (str
    (emitter/column-name emitter source-column)
    " IN ("
    (emitter/->query-string
      {:selection      (emitter/column-name emitter target-column)
       :target-table   target-table 
       :join-statement join-statement})
    " WHERE ?)"))

(defn join-filter-subquery
  [registry item]
  (derive-missing-key item :join-filter-subquery #(join-filter-subquery* registry item)))

(defn compile-join
  [registry attribute]
  (->> attribute
    (join-statement registry)
    (join-source-column registry)
    (join-target-column registry)
    (join-target-table registry)
    (join-filter-subquery registry)))


(comment
  (compile-join {:emitter emitter/default-emitter}
    {:key :person/friend
     :type :join
     :join-path [:person/id :friendship/first-person :friendship/second-person :person/id]})
  (compile-join {:emitter emitter/default-emitter}
    {:key :person/house
     :type :join
     :join-path [:person/id :house/owner-id]}))

(defn compile-joins
  [registry]
  (update registry :attributes
    conditionally-update
    (fn [attr] (and (= :join (:type attr))
                 (not (:aggregate attr))))
    (fn [attr] (compile-join registry attr))))

(comment
  (compile-joins
    {:emitter emitter/default-emitter
     :attributes
     [{:key :foo/bar
       :type :root}
      {:key :person/friend
       :type :join
       :join-path [:person/id :friendship/first-person :friendship/second-person :person/id]}
      {:key :person/house
       :type :join
       :join-path [:person/id :house/owner-id]}]}))

(defn check-duplicate-keys [attrs]
  ;; TODO: implement with loop/recur to
  ;; tell which key is duplicated 
  (when-not (apply distinct? (map :key attrs))
    (throw (ex-info "Duplicate keys" {}))))

(comment
  (check-duplicate-keys [{:key :a/b}
                         {:key :a/c}])
  (check-duplicate-keys [{:key :a/b}
                         {:key :a/c}
                         {:key :a/c}]))

(defn compile-formula
  [registry item]
  ;; TODO: catch exception when joins are used inside formulas
  (derive-missing-key item :compiled-formula
    #(expressions/compile-to-string registry (:formula item))))

(defn compile-formulas
  [{:keys [attributes] :as registry}]
  (->> (conditionally-update attributes
         (fn [attr] (or (:aggregate attr)
                      (= :pseudo-column (:type attr))
                      (= :primary-key (:type attr))))
         (fn [attr] (compile-formula registry attr)))
    (assoc registry :attributes)))

(comment
  (->> {:emitter emitter/default-emitter
        :operators (build-index :key expressions/common-operators)
        :attributes my-attributes}
    compile-formulas
    :attributes
    (filterv #(:formula %))))

(defn find-aggregators
  [attributes]
  (into #{} (map :key (filter :aggregate attributes))))

(comment
  (find-aggregators my-attributes))

(defn find-pseudo-columns
  [attributes]
  (into #{} (map :key (filter #(= :pseudo-column (:type %)) attributes))))

(comment
  (find-pseudo-columns my-attributes))

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

(comment
  (check-dependency-on-aggregators
    #{:a :b}
    [[:x :y]
     [:x :a]])
  (check-dependency-on-aggregators
    #{:b}
    [[:x :y]
     [:x :a]]))

(comment
  (check-circular-dependency!
    (collect-dependencies
      (:attributes
       (compile-formulas
         {:emitter emitter/default-emitter
          :operators (build-index :key expressions/common-operators)
          :attributes my-attributes}))))
  (check-dependency-on-aggregators
    (find-aggregators my-attributes)
    (collect-dependencies
      (:attributes
       (compile-formulas
         {:emitter emitter/default-emitter
          :operators (build-index :key expressions/common-operators)
          :attributes my-attributes})))))

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

(comment
  (collect-independent-pseudo-columns #{:a/b :a/c :a/d :a/e}
   [[:a/b :a/c]
    [:a/b :a/e]
    [:a/c :a/d]])
  (collect-independent-pseudo-columns #{}
   [[:a/b :a/c]
    [:a/b :a/e]
    [:a/c :a/d]]))

(defn get-key [attrs id k]
  (let [x (some #(and (= id (:key %)) %) attrs)]
    (get x k)))

(comment
  (get-key [{:key :foo/item :x 1}] :foo/item :x))

(defn direct-dependents [dependencies independents]
  (->> dependencies
    (filter (fn [[_ y]] (contains? independents y)))
    (map first)
    (into #{})))

(defn get-compiled-formula [attrs ks]
  (->> (for [k ks]
         [k (get-key attrs k :compiled-formula)])
    (into {})))

(defn build-key-index [attrs pred k]
  (into {} (map (juxt :key k) (filter pred attrs))))

(comment
  (build-key-index [{:key :a/x :show 1}
                    {:key :a/y :show 2}]
    #(#{:a/x :a/y} (:key %))
    :show))

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

(comment
  (->> (compile-formulas
         {:emitter emitter/default-emitter
          :operators (build-index :key expressions/common-operators)
          :attributes my-attributes})
    (expand-nested-pseudo-columns)
    :attributes
    (filter #(= :pseudo-column (:type %)))
    (mapv #(select-keys % [:compiled-formula :key]))))

(comment
  (expressions/substitute-atomic-variables
    {:variable-values {:person/age {:raw-string "(?)-(?)"
                                    :params [(expressions/av 'current-year)
                                             (expressions/av :person/yob)]}}}
    {:raw-string "(?)-10"
     :params [(expressions/av :person/age)]}))

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
  (let [jfs (build-key-index attributes #(and (= :join (:type %)) (not (:aggregate %)))
              :join-filter-subquery)]
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
              ;; `ident-value variable is only available here
              (expressions/substitute-atomic-variables
                {:variable-values {`ident-value (expressions/av `ident-value)}})
              (expressions/substitute-atomic-variables
                {:variable-values compiled-pseudo-columns})
              (assoc attr :compiled-ident-filter))))
        (conditionally-update
          #(and (:join-filter %) (not (:compiled-join-filter %)))
          (fn [{condition :join-filter :as attr}]
            (->> condition
              (expressions/compile-to-string registry)
              ;; `source-column-value variable is only available here
              (expressions/substitute-atomic-variables
                {:variable-values {`source-column-value (expressions/av `source-column-value)}})
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

(comment
  (let [registry {:emitter emitter/default-emitter
             :operators (build-index :key expressions/common-operators)
             :attributes my-attributes}]
    (->> registry
      (compile-formulas)
      (expand-nested-pseudo-columns)
      (expand-pseudo-columns-in-aggregators)
      (compile-joins)
      (join-filter-subqueries)
      (compile-filters)
      :attributes
      #_(filter #(= :pseudo-column (:type %)))
      (filter #(:compiled-filter %))
      (mapv #(select-keys % [:compiled-filter :key])))))

(defn compile-variable
  [{k :key :keys [cached? compute] :as attr}]
  (let [f (if cached?
            (fn haha [env _computed-graphs]
              (p/cached env [:walkable/variable-getter k]
                (compute env)))
            (fn hihi [env _computed-graphs]
              (compute env)))]
    (assoc attr :compiled-compute f)))

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
  (merge attr
    {:offset-fallback (pagination/order-by-fallback emitter
                        {:default (:default-offset attr)
                         :validate (:validate-offset attr)
                         :throw? (= :error (:on-invalid-offset attr))})
     :limit-fallback (pagination/order-by-fallback emitter
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
    #(#{:root :join} (:type %))
    #(compile-pagination-fallback registry %)))

(defn ident-filter
  [_registry item]
  (derive-missing-key item :ident-filter (constantly [:= (:key item) (expressions/av `ident-value)])))

(defn derive-ident-cardinality
  [registry]
  (update registry :attributes
    conditionally-update
    (fn [attr] (= :primary-key (:type attr)))
    (fn [attr] (assoc attr :cardinality :one))))

(defn derive-ident-filters
  [registry]
  (update registry :attributes
    conditionally-update
    (fn [attr] (= :primary-key (:type attr)))
    (fn [attr] (ident-filter registry attr))))

(defn join-filter
  ;; TODO: :target-column must exist => derive it first
  [_registry {:keys [:target-column] :as item}]
  ;; TODO: (if (:use-cte item))
  (derive-missing-key item :join-filter (constantly [:= target-column (expressions/av `source-column-value)])))

(defn derive-join-filters
  [registry]
  (update registry :attributes
    conditionally-update
    (fn [attr] (= :join (:type attr)))
    (fn [attr] (join-filter registry attr))))

;; FIXME
(defn compile-group-by
  [compiled-formulas group-by-keys]
  (->> group-by-keys
    (map compiled-formulas)
    (map :raw-string)
    (string/join ", ")
    (str " GROUP BY ")))

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
            (if (= :one (get cardinality k))
              return-one
              return-many))]
    (assoc attr :return f)))

(defn compile-return-functions
  [registry]
  (update registry :attributes
    conditionally-update
    #(#{:root :join} (:type %))
    compile-return-function))

(defn group-registry
  [flat-registry]
  ;; FIXME
  {:emitter emitter/default-emitter
   :operators (build-index :key expressions/common-operators)
   :attributes flat-registry})

(defn collect-outputs [attrs]
  (into #{} (mapcat :output) attrs))

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
        non-true-columns (into #{} (filter #(#{:root :join :pseudo-column} (:type %))) attributes)
        all-columns (set/union (collect-outputs attributes) found-columns)
        true-columns (set/difference all-columns non-true-columns)]
    (merge registry
      {:true-columns true-columns
       :found-variables found-variables})))

(defn fill-true-column-attributes [registry])

(defn compile-clojuric-names
  [{:keys [:emitter] :as registry}]
  )

(defn compile-true-columns
  [{:keys [:emitter] :as registry}]
  )

(defn collect-true-columns [registry])

(defn inline-into
  [k true-column-names]
  (fn [attr]
    (update attr k
      #(expressions/substitute-atomic-variables
         {:variable-values true-column-names} %))))

(defn inline-true-columns
  [{:keys [:attributes] :as registry}]
  (let [column-names (collect-true-columns registry)]
    (assoc registry :attributes
      (-> attributes
        (conditionally-update
          #(:compiled-filter %)
          (inline-into :compiled-filter column-names))
        (conditionally-update
          #(:compiled-ident-filter %)
          (inline-into :compiled-ident-filter column-names))
        (conditionally-update
          #(:compiled-join-filter %)
          (inline-into :compiled-join-filter column-names))
        (conditionally-update
          #(:compiled-having %)
          (inline-into :compiled-having column-names))
        (conditionally-update
          #(:compiled-formula %)
          (inline-into :compiled-formula column-names))))))

(defn compile-selection
  [registry]
  ;; selection for true columns and formulas
  )

(defn compile-join-selection
  [registry])

(defn compile-traverse-scheme [registry]
  ;; FIXME
  #{:traverse-scheme/roots
    :traverse-scheme/joins
    :traverse-scheme/columns})

(defn ident-source-table*
  [{:keys [emitter]} item]
  (derive-missing-key item :table #(emitter/table-name emitter (:key item))))

(defn join-source-table [])

(defn root-source-table [])

(defn rotate [coll]
  (take (count coll) (drop 1 (cycle coll))))

(defn build-indexes [registry])

(defn compile-floor-plan [flat-attributes]
  (let [registry (group-registry flat-attributes)]
    (->> registry
      (compile-formulas)
      (expand-nested-pseudo-columns)
      (expand-pseudo-columns-in-aggregators)
      (compile-joins)
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
      (inline-true-columns)
      (compile-pagination-fallbacks)
      (compile-selection)
      (compile-join-selection)
      #_(compile-cte-keyword)
      (compile-traverse-scheme)
      #_(compile-group-by)
      (build-indexes))))
