(ns walkable.sql-query-builder.filters
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.set :as set]))

(defn namespaced-keyword?
  [x]
  (and (keyword? x) (namespace x)))

(defmulti operator? identity)

(defmethod operator? :default
  [_k]
  false)

(defmulti disallow-no-column? identity)

(defmethod disallow-no-column? :default
  [_k]
  true)

(defmulti valid-params-count?
  (fn [operator n] operator))

(defmethod valid-params-count? :default
  [_operator _n]
  true)

(defn parameterize-tuple [n]
  (str
    "("
    (clojure.string/join ", "
      (repeat n \?))
    ")"))

(defmulti parameterize-operator
  (fn [operator column _params] operator))

;; built-in operators

;; nil?
(defmethod operator? :nil?
  [_k] true)

(defmethod valid-params-count? :nil?
  [_operator n] (= n 0))

(defmethod parameterize-operator :nil?
  [_operator column _params]
  (str column " IS NULL"))

;; not-nil?
(defmethod operator? :not-nil?
  [_k] true)

(defmethod valid-params-count? :not-nil?
  [_operator n] (= n 0))

(defmethod parameterize-operator :not-nil?
  [_operator column _params]
  (str column " IS NOT NULL"))

;; =
(defmethod operator? :=
  [_k] true)

(defmethod valid-params-count? :=
  [_operator n] (= n 1))

(defmethod parameterize-operator :=
  [_operator column _params]
  (str column " = ?"))

;; <
(defmethod operator? :<
  [_k] true)

(defmethod valid-params-count? :<
  [_operator n] (= n 1))

(defmethod parameterize-operator :<
  [_operator column _params]
  (str column " < ?"))

;; >
(defmethod operator? :>
  [_k] true)

(defmethod valid-params-count? :>
  [_operator n] (= n 1))

(defmethod parameterize-operator :>
  [_operator column _params]
  (str column " > ?"))

;; <=
(defmethod operator? :<=
  [_k] true)

(defmethod valid-params-count? :<=
  [_operator n] (= n 1))

(defmethod parameterize-operator :<=
  [_operator column _params]
  (str column " <= ?"))

;; >=
(defmethod operator? :>=
  [_k] true)

(defmethod valid-params-count? :>=
  [_operator n] (= n 1))

(defmethod parameterize-operator :>=
  [_operator column _params]
  (str column " >= ?"))

;; <>
(defmethod operator? :<>
  [_k] true)

(defmethod valid-params-count? :<>
  [_operator n] (= n 1))

(defmethod parameterize-operator :<>
  [_operator column _params]
  (str column " <> ?"))

;; like
(defmethod operator? :like
  [_k] true)

(defmethod valid-params-count? :like
  [_operator n] (= n 1))

(defmethod parameterize-operator :like
  [_operator column _params]
  (str column " LIKE ?"))

;; not=
(defmethod operator? :not=
  [_k] true)

(defmethod valid-params-count? :not=
  [_operator n] (= n 1))

(defmethod parameterize-operator :not=
  [_operator column _params]
  (str column " != ?"))

;; not-like
(defmethod operator? :not-like
  [_k] true)

(defmethod valid-params-count? :not-like
  [_operator n] (= n 1))

(defmethod parameterize-operator :not-like
  [_operator column _params]
  (str column  " NOT LIKE ?"))

;; between
(defmethod operator? :between
  [_k] true)

(defmethod valid-params-count? :between
  [_operator n] (= n 2))

(defmethod parameterize-operator :between
  [_operator column _params]
  (str column " BETWEEN ? AND ?"))

;; not-between
(defmethod operator? :not-between
  [_k] true)

(defmethod valid-params-count? :not-between
  [_operator n] (= n 2))

(defmethod parameterize-operator :not-between
  [_operator column _params]
  (str column " NOT BETWEEN ? AND ?"))

;; in
(defmethod operator? :in
  [_k] true)

;; - in can have any number of params

(defmethod parameterize-operator :in
  [_operator column params]
  (str column " IN " (parameterize-tuple (count params))))

;; not-in
(defmethod operator? :not-in
  [_k] true)

;; - not-in can have any number of params

(defmethod parameterize-operator :not-in
  [_operator column params]
  (str column " NOT IN " (parameterize-tuple (count params))))

(defn mask-unsafe-params
  "Helper function for inline-safe-params. Receives two arguments:
  - params: a vector of condition params
  - column-names: a map of namespaced keywords to their column names

  Returns two lists:
  - masked: the original params list with unsafe one replaced with '?'
  - unmasked: the new params list with only unsafe ones."
  [params column-names]
  (-> (fn [result x]
            (if (or (number? x)
                  (and (namespaced-keyword? x)
                    (get column-names x)))
              (-> result
                (update  :masked conj
                  (if (keyword? x)
                    (get column-names x)
                    x)))
              (-> result
                (update  :masked conj \?)
                (update  :unmasked conj x))))
    (reduce {:masked [] :unmasked []} params)))

;; specs
(s/def ::operators operator?)

(s/def ::condition-value
  #(or (number? %) (string? %) (boolean? %) (namespaced-keyword? %)))

(s/def ::condition
  (s/&
    (s/cat
      :operator ::operators
      :params
      (s/alt
        :params (s/* ::condition-value)
        :params (s/coll-of ::condition-value)))
    (fn [{:keys [operator params]}]
      (valid-params-count? operator
        (count (second params))))))

(s/def ::conditions
  (s/or
    :condition
    ::condition

    :conditions
    (s/cat
      :combinator (s/? #{:and :or})
      :conditions (s/+ ::conditions))))

(defn combine
  [operator conditions]
  (let [conditions (remove nil? conditions)]
    (if (= 1 (count conditions))
      conditions
      (concat
        ["("]
        (interpose (if (= :or operator)
                     " OR "
                     " AND ")
          conditions)
        [")"]))))

(defn combination-match?
  ([k x]
   (and
     (map? x)
     (contains? x k))))

(defn match?
  ([x]
   (and (vector? x)
     (= 2 (count x))
     (keyword? (first x))))
  ([k x]
   (and (vector? x)
     (= 2 (count x))
     (= k (first x)))))

(s/def ::clauses
  (s/or
    :clauses (s/coll-of (s/cat
                          :key #(or (and (keyword? %) (namespace %))
                                  (= % :_))
                          :conditions ::conditions)
               :into [])
    :clauses (s/cat
               :combinator (s/? #{:and :or})
               :clauses (s/+ ::clauses))))

(defn clauses? [x]
  (and (vector? x)
    (= 2 (count x))
    (= :clauses (first x))))

(declare process-multi)
(declare process-clauses)

(defn process-multi
  [{:keys [key keymap] :as env} combinator x]
  (if (and (vector? x)
        (not (match? x)))
    (combine combinator (map #(process-clauses env %) x))
    (process-clauses env x)))

(defn process-clauses
  [{:keys [key keymap] :as env} x]
  (cond
    (match? x)
    (let [coll (second x)]
      (process-multi env :and coll))

    (combination-match? :clauses x)
    (let [{:keys [combinator clauses] k :key} x]
      (process-multi (assoc env :key (or k key))
        combinator clauses))

    (combination-match? :conditions x)
    (let [{:keys [combinator conditions] k :key} x]
      (process-multi (assoc env :key (or k key))
        combinator conditions))

    (combination-match? :operator x)
    (let [{:keys [operator params]} x
          params                    (second params)]
      (when-not (and (= key :_)
                  (disallow-no-column? operator))
        {:condition (parameterize-operator operator
                      (get keymap key) params)
         :params    params}))))

(defn parameterize
  [env clauses]
  (let [all              (flatten (process-clauses env
                                    (s/conform ::clauses clauses)))
        condition-string (apply str
                           (map #(if (map? %) (:condition %) %) all))
        parameters       (flatten (->> all (filter map?) (map :params)))]
    [condition-string parameters]))

(s/def ::namespaced-keyword
  namespaced-keyword?)

(s/def ::column+order-params
  (s/cat
    :column ::namespaced-keyword
    :params (s/* #{:asc :desc :nils-first :nils-last})))

(def order-params->string
  {:asc        " ASC"
   :desc       " DESC"
   :nils-first " NULLS FIRST"
   :nils-last  " NULLS LAST"})

(defn ->order-by-string [column-names order-by]
  (let [form (s/conform (s/+ ::column+order-params) order-by)]
    (when-not (= ::s/invalid form)
      (let [form (filter #(contains? column-names (:column %)) form)]
        (when (seq form)
          (->> form
            (map (fn [{:keys [column params]}]
                   (str
                     (get column-names column)
                     (->> params
                       (map order-params->string)
                       (apply str)))))
            (clojure.string/join ", ")))))))
