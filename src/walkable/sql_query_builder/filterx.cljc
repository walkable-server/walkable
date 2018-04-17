(ns walkable.sql-query-builder.filterx
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cheshire.core :refer [generate-string]]
            [clojure.set :as set]))

;; don't use "?" as raw-string
;; as applying `string/split` to it is not
;; consistent in both Clojure and Clojurescript
;; use " ? " or "(?)" instead
(defn inline-params
  [{:keys [raw-string params]}]
  {:params     (flatten (map :params params))
   :raw-string (->> (conj (mapv :raw-string params) nil)
                 (interleave (string/split raw-string #"\?"))
                 (apply str))})

(defmulti valid-params-count?
  (fn dispatcher [operator n] operator))

(defmethod valid-params-count? :default
  [_operator _n]
  true)

(defmethod valid-params-count? :not
  [_operator n]
  (= n 1))

(defn namespaced-keyword?
  [x]
  (and (keyword? x) (namespace x)))

(s/def ::namespaced-keyword namespaced-keyword?)

(defmulti operator? identity)
(defmethod operator? :default [_operator] false)

(s/def ::operators operator?)

(defmethod operator? :identity [_operator] true)
(defmethod operator? :and [_operator] true)
(defmethod operator? :or [_operator] true)
(defmethod operator? :not [_operator] true)
(defmethod operator? := [_operator] true)
(defmethod operator? :- [_operator] true)
(defmethod operator? :count [_operator] true)
(defmethod operator? :array [_operator] true)
(defmethod operator? :in [_operator] true)
(defmethod operator? :case [_operator] true)

(defmulti unsafe-expression? identity)
(defmethod unsafe-expression? :default [_operator] false)

(s/def ::unsafe-expression unsafe-expression?)

(defmethod unsafe-expression? :cast [_operator] true)
(defmethod unsafe-expression? :json [_operator] true)
(defmethod unsafe-expression? :time [_operator] true)

(s/def ::selection-params
  (s/or
    :number number?
    :column ::namespaced-keyword))

(s/def ::expression
  (s/or
    :number number?
    :boolean boolean?
    :string string?
    :column ::namespaced-keyword
    :expression
    (s/and vector?
      (s/cat :operator (s/? ::operators)
        :params (s/+ ::expression)))
    :unsafe-expression
    (s/and vector?
      (s/cat :operator ::unsafe-expression
        :params (s/+ (constantly true))))
    :join-filters
    (s/and map?
      (s/+
        (s/or :join-filter
          (s/cat :join-key ::namespaced-keyword
            :expression ::expression))))))


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

(defmulti cast-type identity)
(defmethod cast-type :integer [_type] "INTEGER")
(defmethod cast-type :json [_type] "json")
(defmethod cast-type :date [_type] "DATE")
(defmethod cast-type :text [_type] "TEXT")
(defmethod cast-type :default [_type] nil)

(defmethod process-unsafe-expression :cast
  [env [_operator [expression type]]]
  (let [expression (s/conform ::expression expression)
        type-str   (cast-type type)]
    (assert (not= expression ::s/invalid) "Invalid expression")
    (assert type-str "Invalid type")
    (inline-params
      {:raw-string (str "CAST (? AS " type-str ")")
       :params     [(process-expression env expression)]})))

;; dummy
(defmethod process-operator :identity
  [_env [_operator params]]
  {:raw-string "(?)"
   :params params})

(defmethod process-operator :and
  [_env [_operator params]]
  {:raw-string
   (str "("
     (clojure.string/join ") AND ("
       (repeat (count params) \?))
     ")")
   :params params})

(defmethod process-operator :or
  [_env [_operator params]]
  {:raw-string
   (str "("
     (clojure.string/join ") OR ("
       (repeat (count params) \?))
     ")")
   :params params})

(defmethod process-operator :=
  [_env [_operator params]]
  {:raw-string
   "? = ?"
   :params params})

(defmethod process-operator :-
  [_env [_operator params]]
  (assert (not (zero? (count params)))
    "There must be at least one parameter to `-`")
  (if (= 1 (count params))
    {:raw-string "0 - ?"
     :params     params}
    {:raw-string (str "("
                   (clojure.string/join " - " (repeat (count params) \?))
                   ")")
     :params     params}))

(defmethod process-operator :+
  [_env [_operator params]]
  (case (count params)
    0
    {:raw-string "0"
     :params     []}
    1
    {:raw-string " ? "
     :params     params}
    ;; default
    {:raw-string (str "("
                   (clojure.string/join " + " (repeat (count params) \?))
                   ")")
     :params     params}))

(defmethod process-operator :count
  [_env [_operator params]]
  (assert (= 1 (count params)))
  {:raw-string "COUNT (?)"
   :params params})

(defmethod process-operator :in
  [_env [_operator params]]
  {:raw-string (str "(?) IN ("
                 (clojure.string/join ", "
                   ;; decrease by 1 to exclude the first param
                   ;; which should go before `IN`
                   (repeat (dec (count params)) \?))
                 ")")
   :params     params})

(process-operator {} [:and [1 2 3]])
(process-operator {} [:in [1 2 3]])

(defmethod process-operator :not
  [_env [_operator params]]
  {:raw-string "NOT (?)"
   :params params})

(process-operator {} [:not 1])

(defmethod process-expression :expression
  [env [_kw {:keys [operator params] :or {operator :and}}]]
  (assert (valid-params-count? operator (count params))
    (str "Wrong number of arguments to " operator " operator"))
  (inline-params
    (process-operator env
      [operator (mapv #(process-expression env %) params)])))


(defmethod process-expression :unsafe-expression
  [env [_kw {:keys [operator params] :or {operator :and}}]]
  (assert (valid-params-count? operator (count params))
    (str "Wrong number of arguments to " operator " operator"))
  (process-unsafe-expression env [operator params]))

(defmethod process-expression :join-filter
  [env [_kw {:keys [join-key expression]}]]
  (let [subquery (-> env :join-filter-subqueries join-key)]
    (assert subquery (str "No join filter found for join key " join-key))
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

(defmethod process-expression :number
  [_env [_kw number]]
  {:raw-string (str number)
   :params     []})

(defmethod process-expression :boolean
  [_env [_kw value]]
  {:raw-string "?"
   :params     [value]})

(defmethod process-operator :case
  [_env [_kw expressions]]
  (let [n (count expressions)]
    (assert (> n 2))
    (let [when+else-count (dec n)
          else?           (odd? when+else-count)
          when-count      (if else? (dec when+else-count) when+else-count)]
      {:raw-string (str " CASE (?) "
                     (apply str (repeat (/ when-count 2) " WHEN (?) THEN (?) "))
                     (when else? " ELSE (?)")
                     " END")
       :params     expressions})))

(defmethod process-expression :string
  [_env [_kw string]]
  {:raw-string "?"
   :params     string})

(defmethod process-expression :column
  [{:keys [column-names] :as env} [_kw column-keyword]]
  (let [column (get column-names column-keyword)]
    (assert column (str "Invalid colum keyword " column-keyword))
    (if (string? column)
      {:raw-string column
       :params     []}
      (let [form (s/conform ::expression column)]
        (assert (not= ::s/invalid form) (str "Invalid pseudo column for " column-keyword ": " column))
        (inline-params
          {:raw-string " ? "
           :params     [(process-expression env form)]})))))

(inline-params
  {:raw-string "?"
   :params [{:params [], :raw-string "2018 - `human`.`yob`"}]})

(defn parameterize
  [env clauses]
  (let [form (s/conform ::expression clauses)]
    (assert (not= ::s/invalid form) (str "Invalid expression" (pr-str clauses)))
    ;;(println "clauses:" clauses)
    ;;(println "form: " form)
    (process-expression env form)))
#_
(process-expression {:column-names {:foo/bar? "`foo`.`bar`"}}
    (s/conform ::expression [[:= :foo/bar "a"]
                             [:case 1 2 3 4]
                             [:cast "2" :integer]
                             [:not [:= :foo/bar 2]]]))
#_
(process-expression {:column-names {:foo/bar? "`foo`.`bar`"}}
    (s/conform ::expression [[:= :foo/bar "a"]
                             [:cast [:json {:a 1 :b [2 3]}] :json]
                             [:in 1 2 3 4 5]]))

#_
(process-expression {:column-names {:foo/bar "`foo`.`bar`"}
                     :join-filter-subqueries
                     {:a/b "x IN (SELECT blabla WHERE ?)"
                      :c/d "x IN (SELECT blabla WHERE ?)"}}
  (s/conform ::expression [:identity 2]
    #_{:a/b ;;[:= :foo/bar "a"]
       {:c/d [:= :foo/bar "b"]}
       }
                           ;; {:a/b [:cast [:json {:a 1 :b [2 3]}] :json]}
                           ;;{:c/d [:in 1 2 3 4 5]}
                           ))
