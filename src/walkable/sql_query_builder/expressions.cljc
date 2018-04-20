(ns walkable.sql-query-builder.expressions
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            #?(:clj [cheshire.core :refer [generate-string]])
            [clojure.set :as set]))

#?(:cljs
   (defn generate-string
     "Equivalent of cheshire.core/generate-string for Clojurescript"
     [ds]
     (.stringify js/JSON (clj->js ds))))

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

(def conformed-nil
  {:raw-string "NULL"
   :params     []})

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
  (if (empty? params)
    {:raw-string "(?)"
     :params     [{:raw-string " ? "
                   :params     [true]}]}
    {:raw-string (clojure.string/join " AND "
                   (repeat (count params) "(?)"))
     :params     params}))

(defmethod operator? :or [_operator] true)

(defmethod process-operator :or
  [_env [_operator params]]
  (if (empty? params)
    conformed-nil
    {:raw-string (clojure.string/join " OR "
                   (repeat (count params) "(?)"))
     :params     params}))

(defn multiple-compararison
  "Common implementation of process-operator for comparison operators: =, <, >, <=, >="
  [comparator-string params]
  (assert (< 1 (count params))
    (str "There must be at least two arguments to " comparator-string))
  (let [params (partition 2 1 params)]
    {:raw-string (clojure.string/join " AND "
                   (repeat (count params) (str "(?)" comparator-string "(?)")))
     :params     (flatten params)}))

(defmethod operator? := [_operator] true)

(defmethod process-operator :=
  [_env [_operator params]]
  (multiple-compararison "=" params))

(defmethod operator? :> [_operator] true)

(defmethod process-operator :>
  [_env [_operator params]]
  (multiple-compararison ">" params))

(defmethod operator? :>= [_operator] true)

(defmethod process-operator :>=
  [_env [_operator params]]
  (multiple-compararison ">=" params))

(defmethod operator? :< [_operator] true)

(defmethod process-operator :<
  [_env [_operator params]]
  (multiple-compararison "<" params))

(defmethod operator? :<= [_operator] true)

(defmethod process-operator :<=
  [_env [_operator params]]
  (multiple-compararison "<=" params))

(defn infix-notation
  "Common implementation for +, -, *, /"
  [operator-string params]
  {:raw-string (clojure.string/join operator-string
                 (repeat (count params) "(?)"))
   :params     params})

(defmethod operator? :+ [_operator] true)

(defmethod process-operator :+
  [_env [_operator params]]
  (if (empty? params)
    {:raw-string "0"
     :params     []}
    (infix-notation "+" params)))

(defmethod operator? :* [_operator] true)

(defmethod process-operator :*
  [_env [_operator params]]
  (if (empty? params)
    {:raw-string "1"
     :params     []}
    (infix-notation "*" params)))

(defmethod operator? :- [_operator] true)

(defmethod process-operator :-
  [_env [_operator params]]
  (assert (not (empty? params))
    "There must be at least one parameter to `-`")
  (if (= 1 (count params))
    {:raw-string "0-(?)"
     :params     params}
    (infix-notation "-" params)))

(defmethod operator? :/ [_operator] true)

(defmethod process-operator :/
  [_env [_operator params]]
  (assert (not (empty? params))
    "There must be at least one parameter to `/`")
  (if (= 1 (count params))
    {:raw-string "1/(?)"
     :params     params}
    (infix-notation "/" params)))

(defn one-argument-operator
  [params operator-name raw-string]
  (assert (= 1 (count params))
    (str "There must be exactly one argument to `" operator-name "`."))
  {:raw-string raw-string
   :params     params})

(defn multiple-argument-operator
  [params raw-operator]
  (let [n (count params)]
    {:raw-string (str raw-operator "("
                   (clojure.string/join ", "
                     (repeat n \?))
                   ")")
     :params     params}))

(defmethod operator? :count [_operator] true)

(defmethod process-operator :count
  [_env [_operator params]]
  (one-argument-operator params "count" "COUNT (?)"))

(defmethod operator? :not [_operator] true)

(defmethod process-operator :not
  [_env [_operator params]]
  (one-argument-operator params "not" "NOT (?)"))

(defmethod operator? :distinct [_operator] true)

(defmethod process-operator :distinct
  [_env [_operator params]]
  (one-argument-operator params "distinct" "DISTINCT ?"))

(defmethod operator? :str [_operator] true)

(defmethod process-operator :str
  [_env [_operator params]]
  (multiple-argument-operator params "CONCAT"))

(defmethod operator? :subs [_operator] true)

(defmethod process-operator :subs
  [_env [_operator params]]
  (assert (#{2 3} (count params))
    "There must be two or three arguments to `subs`")
  (multiple-argument-operator params "substr"))

(defmethod operator? :format [_operator] true)

(defmethod process-operator :format
  [_env [_operator params]]
  (assert (< 0 (count params))
    "There must be at least one argument to `format`")
  (multiple-argument-operator params "format"))

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
  conformed-nil)

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
    (assert (even? n)
      "`cond` requires an even number of arguments")
    (assert (not= n 0)
      "`cond` must have at least two arguments")
    (process-expression {} [:boolean true])
    {:raw-string (str "CASE"
                   (apply str (repeat (/ n 2) " WHEN (?) THEN (?)"))
                   " END")
     :params     expressions}))

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
