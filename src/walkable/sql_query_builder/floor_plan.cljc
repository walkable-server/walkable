(ns walkable.sql-query-builder.floor-plan
  (:require [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [clojure.set :as set]
            [clojure.string :as string]
            [weavejester.dependency :as dep]
            [clojure.spec.alpha :as s]))

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

(comment
  (= (build-index :key [{:key :foo :a 1}
                        {:key :bar :b 2}])
    {:foo {:key :foo, :a 1}
     :bar {:key :bar, :b 2}}))

(defn build-index-of [k coll]
  (into {} (for [x coll
                 :let [i (:key x)
                       j (get x k)]]
             [i j])))
(comment
  (= (build-index-of :a [{:key :foo :a 1}
                         {:key :bar :a 2}])
    {:foo 1, :bar 2}))

(comment
  (->> [:* [:+ :a/b 3] 6]
    (expressions/compile-to-string
      {:operators (build-index :key expressions/common-operators)})
    (expressions/substitute-atomic-variables
      {:variable-values {:a/b {:raw-string "a.b" :params []}}}))
  (expressions/compile-to-string
    {:operators (build-index :key expressions/common-operators)}
    [:* [:+ :a/b 3] 6])

  (expressions/compile-to-string
    {:operators (build-index :key expressions/common-operators)}
    [:cast :a/c :text]))

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
    :compute (fn [_env] 2021)}
   {:key :person/id
    :type :true-column
    :primary-key true
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

(def prop->func
  {:table-name emitter/table-name
   :column-name emitter/column-name
   :clojuric-name emitter/clojuric-name})

(defn true-column? [attr]
  (= :true-column (:type attr)))

(defn known-true-columns [attributes]
  (->> attributes
    (filterv true-column?)
    (build-index :key)))

(defn keyword-prop
  [{:keys [:emitter :attributes]} k prop]
  (or (get-in (known-true-columns attributes) [k prop])
    (let [f (prop->func prop)]
      (f emitter k))))

(comment
  (keyword-prop
    {:emitter emitter/default-emitter :attributes []}
    :a/b :table-name)
  (keyword-prop
    {:emitter emitter/default-emitter :attributes [{:key :a/b
                                                    :type :true-column
                                                    :table-name "z"}]}
    :a/b :table-name))

(defn join-statement*
  [registry {:keys [:join-path]}]
  (let [[tag] (s/conform ::join-path join-path)]
    (when (= :with-join-table tag)
      (let [[_source _join-source join-target target] join-path]
        (str
          " JOIN " (keyword-prop registry target :table-name)
          " ON " (keyword-prop registry join-target :column-name)
          ;; TODO: what about other operators than `=`?
          " = " (keyword-prop registry target :column-name))))))

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
  [_registry {:keys [:join-path] :as item}]
  (derive-missing-key item :source-column #(first join-path)))

(defn join-target-column
  [_registry {:keys [:join-path] :as item}]
  (derive-missing-key item :target-column #(second join-path)))

(defn join-target-table
  [registry {:keys [:target-column] :as item}]
  (derive-missing-key item :target-table #(keyword-prop registry target-column :table-name)))

(defn join-filter-subquery*
  [registry {:keys [:join-statement :target-table :source-column :target-column]}]
  (str (keyword-prop registry source-column :column-name)
    " IN ("
    (emitter/->query-string
      {:selection (keyword-prop registry target-column :column-name)
       :target-table   target-table 
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
    (fn [attr] (= :join (:type attr)))
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

(comment
  (->> {:emitter emitter/default-emitter
        :operators (build-index :key expressions/common-operators)
        :attributes my-attributes}
    compile-formulas
    :attributes
    (filterv #(:formula %))))

(defn keyset [coll]
  (->> coll
    (map :key)
    (into #{})))

(defn find-aggregators
  [attributes]
  (->> attributes
    (filter :aggregate)
    (keyset)))

(comment
  (find-aggregators my-attributes))

(defn find-pseudo-columns
  [attributes]
  (->> attributes
    (filter #(= :pseudo-column (:type %)))
    (keyset)))

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

(defn get-prop [attrs k prop]
  (let [x (some #(and (= k (:key %)) %) attrs)]
    (get x prop)))

(comment
  (get-prop [{:key :foo/item :x 1}] :foo/item :x))

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
  (let [jfs (->> attributes
              (filter #(and (= :join (:type %)) (not (:aggregate %))))
              (build-index-of :join-filter-subquery))]
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
  (merge attr
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
    #(and (#{:root :join} (:type %)) (not (:aggregate %)))
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

(defn join-filter
  ;; Note: :target-column must exist => derive it first
  [_registry {:keys [:target-column] :as item}]
  ;; TODO: (if (:use-cte item))
  (derive-missing-key item :join-filter (constantly [:= target-column (expressions/av `source-column-value)])))

(defn derive-join-filters
  [registry]
  (update registry :attributes
    conditionally-update
    (fn [attr] (= :join (:type attr)))
    (fn [attr] (join-filter registry attr))))

(defn compile-group-by
  [compiled-formulas group-by-keys]
  ;; FIXME
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
     :operators (build-index :key operator-set)
     :attributes flat-registry}))

(defn collect-outputs [attrs]
  (into #{} (mapcat :output) attrs))

(defn collect-join-paths [attrs]
  (into #{} (mapcat :join-path) attrs))

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
                       (build-index-of :compiled-formula))]
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
    #(#{:true-column :pseudo-column} (:type %))
    (fn [{:keys [:compiled-formula :clojuric-name] :as attr}]
      (let [selection (expressions/selection compiled-formula clojuric-name)]
        (assoc attr :compiled-selection selection)))))

(defn compile-join-aggregator-selection
  [{:keys [:emitter] :as registry}]
  (update registry :attributes
    conditionally-update
    #(and (= :join (:type %)) (:aggregate %))
    (fn [{:keys [:compiled-formula :clojuric-name] :as attr}]
      (let [{:keys [:target-column]} attr

            aggregator-selection
            (expressions/selection compiled-formula clojuric-name)

            source-column-selection
            (expressions/selection
              {:raw-string "?"
               :params [(expressions/av `source-column-value)]}
              (emitter/clojuric-name emitter target-column))]
        (assoc attr :compiled-join-aggregator-selection
          (expressions/concatenate #(clojure.string/join ", " %)
            [source-column-selection aggregator-selection]))))))

(defn compile-join-selection
  [{:keys [:emitter] :as registry}]
  (update registry :attributes
    conditionally-update
    #(= :join (:type %))
    (fn [{:keys [:target-column] :as attr}]
      (assoc attr :selection
        (expressions/selection
          {:raw-string "?"
           :params [(expressions/av `source-column-value)]}
          (emitter/clojuric-name emitter target-column))))))

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
    (fn [{:keys [:return :target-column :source-column] :as attr}]
      (assoc attr :merge-sub-entities
        (fn ->merge-sub-entities [result-key]
          (fn merge-sub-entities [entities sub-entities]
            (if (empty? sub-entities)
              entities
              (let [groups (group-by target-column sub-entities)]
                (mapv (fn [entity] (let [source-column-value (get entity source-column)]
                                     (assoc entity result-key (return (get groups source-column-value)))))
                  entities)))))))))

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

(defn source-column-variable-values
  [v]
  {:variable-values {`source-column-value
                     (expressions/compile-to-string {} v)}})

(defn compile-query-multiplier
  [{:keys [batch-query] :as registry}]
  (update registry :attributes
    conditionally-update
    #(= :join (:type %))
    (fn [{:keys [:source-column] :as attr}]
      (assoc attr :query-multiplier
        (fn ->query-multiplier [individual-query-template]
          (let [xform (comp (map #(get % source-column))
                        (remove nil?)
                        (map #(-> (expressions/substitute-atomic-variables
                                    (source-column-variable-values %)
                                    individual-query-template)
                                ;; attach source-column-value as meta data
                                (with-meta {:source-column-value %}))))]
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
          (mapv (fn [{k :key :keys [:output :source-column]}]
                  {::pc/input #{source-column}
                   ::pc/output [{k output}]})))

        join-aggregators
        (->> attributes
          (filter #(and (= :join (:type %)) (:aggregate %)))
          (mapv (fn [{k :key :keys [:source-column]}]
                  {::pc/input #{source-column}
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
    {:target-table (merge (build-index-of :target-table (filter #(= :join (:type %)) attributes))
                     (build-index-of :table (filter #(or (= :root (:type %)) (:primary-key %)) attributes)))}
    {:target-column (build-index-of :target-column (filter #(= :join (:type %)) attributes))}
    {:source-column (build-index-of :source-column (filter #(= :join (:type %)) attributes))}
    {:merge-sub-entities (build-index-of :merge-sub-entities (filter #(or (#{:root :join} (:type %)) (:primary-key %)) attributes))}
    {:query-multiplier (build-index-of :query-multiplier (filter :query-multiplier attributes))}
    {:join-statement (build-index-of :join-statement (filter :join-statement attributes))}
    {:compiled-join-selection (build-index-of :compiled-join-selection (filter :compiled-join-selection attributes))}
    {:compiled-join-aggregator-selection (build-index-of :compiled-join-aggregator-selection (filter :compiled-join-aggregator-selection attributes))}
    {:keyword-type (build-index-of :traverse-scheme (filter #(#{:root :join :true-column :pseudo-column} (:type %)) attributes))} 
    {:compiled-pagination-fallbacks (build-index-of :compiled-pagination-fallbacks (filter :compiled-pagination-fallbacks attributes))}
    {:return (build-index-of :return (filter #(or (#{:root :join} (:type %)) (:primary-key %)) attributes))}
    {:aggregator-keywords (into #{} (comp (filter #(:aggregate %)) (map :key)) attributes)}
    {:compiled-variable-getter (build-index-of :compiled-variable-getter (filter :compiled-variable-getter attributes))}
    {:all-filters (build-index-of :all-filters (filter :all-filters attributes))}
    {:ident-keywords (into #{} (comp (filter #(:primary-key %)) (map :key)) attributes)}
    {:compiled-selection (build-index-of :compiled-selection (filter :compiled-selection attributes))}
    {:compiled-formulas (build-index-of :compiled-formula (filter #(#{:true-column :pseudo-column} (:type %)) attributes))})
  )

(defn compact [registry]
  (merge (select-keys registry [:emitter :operators :batch-query])
    {:floor-plan (floor-plan registry)
     :inputs-outputs (inputs-outputs registry)}))

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
      #_ (compile-group-by)
      )))

(defn compile-floor-plan
  [flat-attributes]
  (->> flat-attributes
    (compile-floor-plan*)
    (compact)))

