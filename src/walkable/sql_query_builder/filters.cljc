(ns walkable.sql-query-builder.filters
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cheshire.core :refer [generate-string]]
            [clojure.set :as set]))

(defn inline-params
  [{:keys [raw-string params]}]
  {:params     (flatten (map :params params))
   :raw-string (->> (conj (mapv :raw-string params) nil)
                 (interleave (string/split (if (= "?" raw-string)
                                             " ? "
                                             raw-string)
                               #"\?"))
                 (apply str))})

(defn namespaced-keyword?
  [x]
  (and (keyword? x) (namespace x)))

(s/def ::namespaced-keyword namespaced-keyword?)

(defmulti operator? identity)
(defmethod operator? :default [_operator] false)

(s/def ::operators operator?)

(defmulti unsafe-expression? identity)
(defmethod unsafe-expression? :default [_operator] false)

(s/def ::unsafe-expression unsafe-expression?)

(comment
  (defmethod operator? :array [_operator] true)
  (defmethod unsafe-expression? :json [_operator] true)
  (defmethod unsafe-expression? :time [_operator] true))

(s/def ::expression
  (s/or
    :nil nil?
    :number number?
    :boolean boolean?
    :string string?
    :column ::namespaced-keyword
    :expression
    (s/and vector?
      (s/cat :operator (s/? ::operators)
        :params (s/* ::expression)))
    :unsafe-expression
    (s/and vector?
      (s/cat :operator ::unsafe-expression
        :params (s/* (constantly true))))
    :join-filters
    (s/and map?
      (s/+
        (s/or :join-filter
          (s/cat :join-key ::namespaced-keyword
            :expression ::expression))))))

;; the rule for parentheses in :raw-string
;; outer raw string should provide them
;; inner ones shouldn't

(defmulti process-operator
  (fn dispatcher [_env [operator _params]] operator))

(defmulti process-unsafe-expression
  (fn dispatcher [_env [operator _params]] operator))

(defmulti process-expression
  (fn dispatcher [_env [kw _expression]] kw))

(defmethod process-unsafe-expression :json
  [_env [_operator [json]]]
  (let [json-string (generate-string json)]
    {:raw-string "?"
     :params     [json-string]}))

(defmulti cast-type
  "Registers a valid type for for :cast-type."
  identity)

(defmethod cast-type :integer [_type] "INTEGER")
(defmethod cast-type :json [_type] "json")
(defmethod cast-type :date [_type] "DATE")
(defmethod cast-type :text [_type] "TEXT")
(defmethod cast-type :default [_type] nil)

(defmethod unsafe-expression? :cast [_operator] true)

(defmethod process-unsafe-expression :cast
  [env [_operator [expression type]]]
  (let [expression (s/conform ::expression expression)
        type-str   (cast-type type)]
    (assert (not= expression ::s/invalid)
      (str "First argument to `cast` is not an invalid expression."))
    (assert type-str
      (str "Invalid type to `cast`. You may want to implement `cast-type` for the given type."))
    (inline-params
      {:raw-string (str "CAST (? AS " type-str ")")
       :params     [(process-expression env expression)]})))

(defmethod operator? :and [_operator] true)

(defmethod process-operator :and
  [_env [_operator params]]
  (case (count params)
    0
    {:raw-string "(?)"
     :params [{:raw-string " ? "
               :params [true]}]}
    1
    {:raw-string "(?)" :params params}
    ;; default
    {:raw-string
     (clojure.string/join " AND "
       (repeat (count params) "(?)"))
     :params params}))

(defmethod operator? :or [_operator] true)

(defmethod process-operator :or
  [_env [_operator params]]
  {:raw-string
   (clojure.string/join " OR "
     (repeat (count params) "(?)"))

   :params params})

(defn multiple-compararison
  "Common implementation of process-operator for comparison operators: =, <, >, <=, >="
  [single-comparison-string params]
  (case (count params)
    (0 1)
    {:raw-string "(?)"
     :params [{:raw-string " ? "
               :params [true]}]}
    (let [params (partition 2 1 params)]
      {:raw-string
       (clojure.string/join " AND "
         (repeat (count params) single-comparison-string))

       :params (flatten params)})))

(defmethod operator? := [_operator] true)

(defmethod process-operator :=
  [_env [_operator params]]
  (multiple-compararison "(?) = (?)" params))

(defmethod operator? :> [_operator] true)

(defmethod process-operator :>
  [_env [_operator params]]
  (multiple-compararison "(?) > (?)" params))

(defmethod operator? :>= [_operator] true)

(defmethod process-operator :>=
  [_env [_operator params]]
  (multiple-compararison "(?) >= (?)" params))

(defmethod operator? :< [_operator] true)

(defmethod process-operator :<
  [_env [_operator params]]
  (multiple-compararison "(?) < (?)" params))

(defmethod operator? :<= [_operator] true)

(defmethod process-operator :<=
  [_env [_operator params]]
  (multiple-compararison "(?) <= (?)" params))

(defn infix-notation [operator-string params]
  {:raw-string (clojure.string/join operator-string
                 (repeat (count params) "(?)"))

   :params     params})

(defmethod operator? :+ [_operator] true)

(defmethod process-operator :+
  [_env [_operator params]]
  (case (count params)
    0
    {:raw-string "0"
     :params     []}
    1
    {:raw-string "(?)"
     :params     params}
    ;; default
    (infix-notation "+" params)))

(defmethod operator? :* [_operator] true)

(defmethod process-operator :*
  [_env [_operator params]]
  (case (count params)
    0
    {:raw-string "1"
     :params     []}
    1
    {:raw-string "(?)"
     :params     params}
    ;; default
    (infix-notation "*" params)))

(defmethod operator? :- [_operator] true)

(defmethod process-operator :-
  [_env [_operator params]]
  (assert (not (zero? (count params)))
    "There must be at least one parameter to `-`")
  (if (= 1 (count params))
    {:raw-string "0-(?)"
     :params     params}
    (infix-notation "-" params)))

(defmethod operator? :/ [_operator] true)

(defmethod process-operator :/
  [_env [_operator params]]
  (assert (not (zero? (count params)))
    "There must be at least one parameter to `/`")
  (if (= 1 (count params))
    {:raw-string "1/(?)"
     :params     params}
    (infix-notation "/" params)))

(defmethod operator? :count [_operator] true)

(defmethod process-operator :count
  [_env [_operator params]]
  (assert (= 1 (count params))
    "There must be exactly one argument to `count`.")
  {:raw-string "COUNT (?)"
   :params     params})

(defmethod operator? :in [_operator] true)

(defmethod process-operator :in
  [_env [_operator params]]
  {:raw-string (str "(?) IN ("
                 (clojure.string/join ", "
                   ;; decrease by 1 to exclude the first param
                   ;; which should go before `IN`
                   (repeat (dec (count params)) \?))
                 ")")
   :params     params})

(defmethod operator? :not [_operator] true)

(defmethod process-operator :not
  [_env [_operator params]]
  {:raw-string "NOT (?)"
   :params params})

(defmethod process-expression :expression
  [env [_kw {:keys [operator params] :or {operator :and}}]]
  (inline-params
    (process-operator env
      [operator (mapv #(process-expression env %) params)])))


(defmethod process-expression :unsafe-expression
  [env [_kw {:keys [operator params] :or {operator :and}}]]
  (process-unsafe-expression env [operator params]))

(defmethod process-expression :join-filter
  [env [_kw {:keys [join-key expression]}]]
  (let [subquery (-> env :join-filter-subqueries join-key)]
    (assert subquery
      (str "No join filter found for join key " join-key))
    (inline-params
      {:raw-string subquery
       :params     [(process-expression env expression)]})))

(defmethod process-expression :join-filters
  [env [_kw join-filters]]
  (inline-params
    {:raw-string (str "("
                   (clojure.string/join ") AND ("
                     (repeat (count join-filters) \?))
                   ")")
     :params     (mapv #(process-expression env %) join-filters)}))

(defmethod process-expression :nil
  [_env [_kw number]]
  {:raw-string "NULL"
   :params     []})

(defmethod process-expression :number
  [_env [_kw number]]
  {:raw-string (str number)
   :params     []})

(defmethod process-expression :boolean
  [_env [_kw value]]
  {:raw-string " ? "
   :params     [value]})

(defmethod process-expression :string
  [_env [_kw string]]
  {:raw-string " ? "
   :params     string})

(defmethod process-expression :column
  [{:keys [column-names] :as env} [_kw column-keyword]]
  (let [column (get column-names column-keyword)]
    (assert column
      (str "Invalid column keyword " column-keyword
        ". You may want to add it to schema."))
    (if (string? column)
      {:raw-string column
       :params     []}
      (let [form (s/conform ::expression column)]
        (assert (not= ::s/invalid form)
          (str "Invalid pseudo column for " column-keyword ": " column))
        (inline-params
          {:raw-string "(?)"
           :params     [(process-expression env form)]})))))

(defmethod operator? :case [_operator] true)

(defmethod process-operator :case
  [_env [_kw expressions]]
  (let [n (count expressions)]
    (assert (> n 2)
      "`case` must have at least three arguments")
    (let [when+else-count (dec n)
          else?           (odd? when+else-count)
          when-count      (if else? (dec when+else-count) when+else-count)]
      {:raw-string (str "CASE (?)"
                     (apply str (repeat (/ when-count 2) " WHEN (?) THEN (?)"))
                     (when else? " ELSE (?)")
                     " END")
       :params     expressions})))

(defmethod operator? :cond [_operator] true)

(defmethod process-operator :cond
  [_env [_kw expressions]]
  (let [n (count expressions)]
    (assert (> n 1)
      "`cond` must have at least two arguments")
    (let [when+else-count n
          else?           (odd? when+else-count)
          when-count      (if else? (dec when+else-count) when+else-count)]
      {:raw-string (str "CASE"
                     (apply str (repeat (/ when-count 2) " WHEN (?) THEN (?)"))
                     (when else? " ELSE (?)")
                     " END")
       :params     expressions})))

(defmethod operator? :if [_operator] true)

(defmethod process-operator :if
  [_env [_kw expressions]]
  (let [n (count expressions)]
    (assert (#{2 3} n)
      "`if` must have either two or three arguments")
    (let [else?           (= 3 n)]
      {:raw-string (str "CASE WHEN (?) THEN (?)"
                     (when else? " ELSE (?)")
                     " END")
       :params     expressions})))

(defmethod operator? :when [_operator] true)

(defmethod process-operator :when
  [_env [_kw expressions]]
  (let [n (count expressions)]
    (assert (= 2 n)
      "`when` must have exactly two arguments")
    (let [else?           (= 3 n)]
      {:raw-string "CASE WHEN (?) THEN (?) END"
       :params     expressions})))

(defn parameterize
  [env clauses]
  (let [form (s/conform ::expression clauses)]
    (assert (not= ::s/invalid form)
      (str "Invalid expression: " clauses))
    ;;(println "clauses:" clauses)
    ;;(println "form: " form)
    (process-expression env form)))
