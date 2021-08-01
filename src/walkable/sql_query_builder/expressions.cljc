(ns walkable.sql-query-builder.expressions
  (:require #?(:clj [cheshire.core :refer [generate-string]])
            [clojure.spec.alpha :as s]
            [clojure.string :as string]))

#?(:cljs
   (defn generate-string
     "Equivalent of cheshire.core/generate-string for Clojurescript"
     [ds]
     (.stringify js/JSON (clj->js ds))))

(defrecord AtomicVariable [name])

(defn av [n]
  (AtomicVariable. n))

(defn atomic-variable? [x]
  (instance? AtomicVariable x))

(declare inline-params)

(defn namespaced-keyword?
  [x]
  (and (keyword? x) (namespace x)))

(defn unnamespaced-keyword?
  [x]
  (and (keyword? x) (not (namespace x))))

(s/def ::namespaced-keyword namespaced-keyword?)

(s/def ::unnamespaced-keyword unnamespaced-keyword?)

(defprotocol EmittableAtom
  (emit [this]))

(defn emittable-atom? [x]
  (satisfies? EmittableAtom x))

(defn verbatim-raw-string [s]
  {:raw-string s
   :params     []})

(defn single-raw-string [x]
  {:raw-string "?"
   :params     [x]})

(def conformed-nil
  (verbatim-raw-string "NULL"))

(def conformed-true
  (verbatim-raw-string "TRUE"))

(def conformed-false
  (verbatim-raw-string "FALSE"))

(extend-protocol EmittableAtom
  #?(:clj Boolean :cljs boolean)
  (emit [boolean-val]
    (if boolean-val conformed-true conformed-false))
  #?(:clj Number :cljs number)
  (emit [number]
    (verbatim-raw-string (str number)))
  #?(:clj String :cljs string)
  (emit [string]
    (single-raw-string string))
  #?(:clj java.util.UUID :cljs UUID)
  (emit [uuid]
    (single-raw-string uuid))
  nil
  (emit [a-nil] conformed-nil))

(s/def ::expression
  (s/or
   :atomic-variable atomic-variable?
   :symbol symbol?

   :emittable-atom emittable-atom?

   :column ::namespaced-keyword

   :expression
   (s/and vector?
          (s/cat :operator ::unnamespaced-keyword
                 :params (s/* (constantly true))))
   :join-filters
   (s/coll-of
    (s/or :join-filter
          (s/cat :join-key ::namespaced-keyword
                 :expression ::expression))
    :min-count 1
    :kind map?
    :into [])))

;; the rule for parentheses in :raw-string
;; outer raw string should provide them
;; inner ones shouldn't

(defmulti process-expression
  (fn dispatcher [_env [kw _expression]] kw))

(defmethod process-expression :emittable-atom
  [_env [_kw val]]
  (emit val))

(defn infix-notation
  "Common implementation for +, -, *, /"
  [operator-string params]
  {:raw-string (string/join operator-string
                 (repeat (count params) "(?)"))
   :params     params})

(defn multiple-compararison
  "Common implementation of process-operator for comparison operators: =, <, >, <=, >="
  [comparator-string params]
  (assert (< 1 (count params))
    (str "There must be at least two arguments to " comparator-string))
  (let [params (partition 2 1 params)]
    {:raw-string (string/join " AND "
                   (repeat (count params) (str "(?)" comparator-string "(?)")))
     :params     (flatten params)}))

(def common-cast-type->string
  {:integer "integer"
   :text "text"
   :date "date"
   :datetime "datetime"})

(defn compile-cast-type [cast-type->string]
  (fn [env [_operator [expression type-params]]]
    (let [expression (s/conform ::expression expression)
          type-str (cast-type->string type-params)]
      (assert (not (s/invalid? expression))
        (str "First argument to `cast` must be a valid expression."))
      (assert type-str
        (str "Invalid type to `cast`. You may want to implement `cast-type` for the given type."))
      (inline-params env
        {:raw-string (str "CAST (? AS " type-str ")")
         :params [(process-expression env expression)]}))))

(defn default-sql-name
  [key]
  (let [symbol-name (name key)]
    (string/replace symbol-name #"-" "_")))

(defn infix-operator-fn
  [{:keys [arity sql-name] operator :key}]
  (case arity
    2
    (fn [_env [_operator params]]
      (assert (= 2 (count params))
        (str "There must exactly two arguments to " operator))
      {:raw-string (str "(?)" sql-name "(?)")
       :params     params})
    (fn [_env [_operator params]]
       (let [n (count params)]
         {:raw-string (string/join sql-name
                        (repeat n "(?)"))
          :params     params}))))

(defn postfix-operator-fn
  [{:keys [arity sql-name] :or {arity 1} operator :key}]
  (assert (= 1 arity)
    (str  "Postfix operators always have arity 1. Please check operator " operator "'s definition."))
  (fn [_env [_operator params]]
    (assert (= 1 (count params))
      (str "There must be exactly one argument to " operator))
    {:raw-string (str "(?)" sql-name)
     :params     params}))

(defn no-params-operator-fn
  [{:keys [arity sql-name] :or {arity 0} operator :key}]
  (assert (= 0 arity)
    (str  "Postfix operators always have arity 0. Please check operator " operator "'s definition."))
  (fn [_env [_operator params]]
    (assert (= 0 (count params))
      (str "There must be no argument to " operator))
    {:raw-string sql-name
     :params     params}))

(defn prefix-operator-fn
  [{:keys [arity sql-name] operator :key}]
  (case arity
    0
    (fn [_env [_operator params]]
      (assert (zero? (count params))
        (str "There must be no argument to " operator))
      {:raw-string (str sql-name "()")
       :params     []})
    1
    (fn [_env [_operator params]]
      (assert (= 1 (count params))
        (str "There must exactly one argument to " operator))
      {:raw-string (str sql-name "(?)")
       :params     params})
    ;; else
    (fn [_env [_operator params]]
      (let [n (count params)]
        {:raw-string (str (str sql-name "(")
                       (string/join ", "
                         (repeat n \?))
                       ")")
         :params     params}))))

(defn plain-operator-fn
  [{:keys [:key :sql-name :params-position] :as opts}]
  (let [opts (if sql-name
               opts
               (assoc opts :sql-name (default-sql-name key)))]
    (case params-position
      :infix
      (infix-operator-fn opts)
      :postfix
      (postfix-operator-fn opts)
      :no-params
      (no-params-operator-fn opts)
      ;; default
      (prefix-operator-fn opts))))

(defn plain-operator [opts]
  {:key (:key opts)
   :type :operator
   :compile-args true
   :compile-fn (plain-operator-fn opts)})

(def common-operators
  [{:key :and
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (if (empty? params)
        (single-raw-string true)
        {:raw-string (string/join " AND "
                       (repeat (count params) "(?)"))
         :params     params}))}

   {:key :or
    :type :operator
    :compile-args true
    :compile-fn
    (fn
      [_env [_operator params]]
      (if (empty? params)
        (single-raw-string false)
        {:raw-string (string/join " OR "
                       (repeat (count params) "(?)"))
         :params     params}))}

   {:key :=
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (multiple-compararison "=" params))}

   {:key :>
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (multiple-compararison ">" params))}

   {:key :>=
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (multiple-compararison ">=" params))}

   {:key :<
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (multiple-compararison "<" params))}

   {:key :<=
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (multiple-compararison "<=" params))}

   {:key :+
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (if (empty? params)
        {:raw-string "0"
         :params     []}
        (infix-notation "+" params)))}

   {:key :*
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (if (empty? params)
        {:raw-string "1"
         :params     []}
        (infix-notation "*" params)))}

   {:key :-
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (assert (not-empty params)
        "There must be at least one parameter to `-`")
      (if (= 1 (count params))
        {:raw-string "0-(?)"
         :params     params}
        (infix-notation "-" params)))}

   {:key :/
    :type :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (assert (not-empty params)
        "There must be at least one parameter to `/`")
      (if (= 1 (count params))
        {:raw-string "1/(?)"
         :params     params}
        (infix-notation "/" params)))}

   {:key          :in
    :type         :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      ;; decrease by 1 to exclude the first param
      ;; which should go before `IN`
      (let [n (dec (count params))]
        (assert (pos? n) "There must be at least two parameters to `:in`")
        {:raw-string (str "(?) IN ("
                       (string/join ", "
                         (repeat n \?))
                       ")")
         :params     params}))}
   {:key          :tuple
    :type         :operator
    :compile-args true
    :compile-fn
    (fn [_env [_operator params]]
      (let [n (count params)]
        (assert (pos? n) "There must be at least one parameter to `:tuple`")
        {:raw-string (str "("
                       (string/join ", "
                         (repeat (count params) \?))
                       ")")
         :params     params}))}

   {:key          :case
    :type         :operator
    :compile-args true
    :compile-fn
    (fn
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
           :params     expressions})))}
   {:key          :cond
    :type         :operator
    :compile-args true
    :compile-fn
    (fn [_env [_kw expressions]]
      (let [n (count expressions)]
        (assert (even? n)
          "`cond` requires an even number of arguments")
        (assert (not= n 0)
          "`cond` must have at least two arguments")
        {:raw-string (str "CASE"
                       (apply str (repeat (/ n 2) " WHEN (?) THEN (?)"))
                       " END")
         :params     expressions}))}
   {:key          :if
    :type         :operator
    :compile-args true
    :compile-fn
    (fn [_env [_kw expressions]]
      (let [n (count expressions)]
        (assert (#{2 3} n)
          "`if` must have either two or three arguments")
        (let [else?           (= 3 n)]
          {:raw-string (str "CASE WHEN (?) THEN (?)"
                         (when else? " ELSE (?)")
                         " END")
           :params     expressions})))}
   {:key          :when
    :type         :operator
    :compile-args true
    :compile-fn
    (fn [_env [_kw expressions]]
      (let [n (count expressions)]
        (assert (= 2 n)
          "`when` must have exactly two arguments")
        {:raw-string "CASE WHEN (?) THEN (?) END"
         :params     expressions}))}
   (plain-operator {:key :count-*
                    :sql-name "COUNT(*)"
                    :params-position :no-params})
   (plain-operator {:key :sum
                    :arity 1})
   (plain-operator {:key :count
                    :arity 1})
   (plain-operator {:key :not
                    :arity 1})
   (plain-operator {:key :avg
                    :arity 1})
   (plain-operator {:key :bit-not
                    :arity 1
                    :sql-name "~"})
   (plain-operator {:key :now
                    :arity 0})
   (plain-operator {:key :format})
   (plain-operator {:key :min})
   (plain-operator {:key :max})
   (plain-operator {:key :str
                    :sql-name "CONCAT"})
   (plain-operator {:key :nil?
                    :params-position :postfix
                    :sql-name " is null"})
   (plain-operator {:key :like
                    :params-position :infix})
   (plain-operator {:key :bit-and
                    :params-position :infix
                    :sql-name "&"})
   (plain-operator {:key :bit-or
                    :params-position :infix
                    :sql-name "|"})
   (plain-operator {:key :bit-shift-left
                    :arity 2
                    :params-position :infix
                    :sql-name "<<"})
   (plain-operator {:key :bit-shift-right
                    :arity 2
                    :params-position :infix
                    :sql-name ">>"})])

(def postgres-operator-set
  (concat (mapv #(plain-operator {:key %})
                [:array-append
                 :array-cat
                 :array-fill
                 :array-length
                 :array-lower
                 :array-position
                 :array-positions
                 :array-prepend
                 :array-remove
                 :array-replace
                 :array-to-string
                 :array-upper
                 :cardinality
                 :string-to-array
                 :unnest])
          ;; Use long names instead of "?", "?|"
          ;; Source: https://stackoverflow.com/questions/30629076/how-to-escape-the-question-mark-operator-to-query-postgresql-jsonb-type-in-r
          (mapv #(plain-operator {:key %})
                [:to-char :to-date :to-number :to-timestamp
                 :iso-timestamp :json-agg :jsonb-contains
                 :jsonb-exists :jsonb-exists-any :jsonb-exists-all :jsonb-delete-path])
          (mapv #(plain-operator {:key % :arity 1})
                [:array-ndims :array-dims])

          ;; Source:
          ;; https://www.postgresql.org/docs/current/static/functions-json.html
          (for [[k sql-name]
                {:get "->"
                 :get-as-text "->>"
                 :get-in "#>"
                 :get-in-as-text "#>>"
                 :contains "@>"
                 :overlap "&&"}]
            (plain-operator {:key k
                             :arity 2
                             :params-position :infix
                             :sql-name sql-name}))
          [(plain-operator {:key :concat
                            :params-position :infix
                            :sql-name "||"})

           {:key :array
            :type :operator
            :compile-args true
            :compile-fn
            (fn [_env [_operator params]]
              {:raw-string (str "ARRAY["
                                (string/join ", "
                                             (repeat (count params) "?"))
                                "]")
               :params params})}

           {:key :json*
            :type :operator
            :compile-args true
            :compile-fn
            (fn [_env [_operator [json]]]
              (let [json-string (generate-string json)]
                {:raw-string "?"
                 :params [json-string]}))}

           {:key :json
            :type :operator
            :compile-args true
            :compile-fn
            (fn [_env [_operator [json]]]
              (let [json-string (generate-string json)]
                {:raw-string "?::json"
                 :params [json-string]}))}

           {:key :jsonb
            :type :operator
            :compile-args true
            :compile-fn
            (fn [_env [_operator [json]]]
              (let [json-string (generate-string json)]
                {:raw-string "?::jsonb"
                 :params [json-string]}))}

           {:key :cast
            :type :operator
            :compile-args false
            :compile-fn (compile-cast-type
                         (merge common-cast-type->string
                                {:json "json"
                                 :jsonb "jsonb"}))}]))

(def predefined-operator-sets
  ;; TODO: different :cast operators
  {:postgres (into common-operators postgres-operator-set)
   :mysql common-operators
   :sqlite common-operators})

(defn build-operator-set
  [{:keys [:base :except] :or {base :postgres}}]
  (remove #((set except) (:key %)) (get predefined-operator-sets base)))

(defmethod process-expression :expression
  [{:keys [:operators] :as env}
   [_kw {:keys [:operator :params] :or {operator :and}}]]
  (if-let [operator-config (get operators operator)]
    (let [{:keys [:compile-args :compile-fn]} operator-config]
      (if compile-args
        (let [conformed-params (mapv #(s/conform ::expression %) params)]
          (if-let [failed (some s/invalid? conformed-params)]
            (throw (ex-info (str "Invalid expression: " (pr-str failed))
                     {:type :invalid-expression
                      :expression params}))
            (let [compiled-params (mapv #(process-expression env %) conformed-params)]
              (inline-params env
                (compile-fn env [operator compiled-params])))))
        (compile-fn env [operator params])))
    (throw (ex-info (str "Unknow operator: " operator)
             {:type :unknow-operator
              :name operator}))))

(defmethod process-expression :join-filter
  [env [_kw {:keys [join-key expression]}]]
  (let [subquery (-> env :join-filter-subqueries join-key)]
    (assert subquery
      (str "No join filter found for join key " join-key))
    (inline-params env
      {:raw-string subquery
       :params     [(process-expression env expression)]})))

(defmethod process-expression :join-filters
  [env [_kw join-filters]]
  (inline-params env
    {:raw-string (str "("
                   (string/join ") AND ("
                     (repeat (count join-filters) \?))
                   ")")
     :params     (mapv #(process-expression env %) join-filters)}))

(defmethod process-expression :atomic-variable
  [_env [_kw atomic-variable]]
  (single-raw-string atomic-variable))

(defmethod process-expression :column
  [_env [_kw column-keyword]]
  (single-raw-string (AtomicVariable. column-keyword)))

(defmethod process-expression :symbol
  [_env [_kw sym]]
  (single-raw-string (AtomicVariable. sym)))

(defn substitute-atomic-variables
  [{:keys [variable-values] :as env} {:keys [raw-string params]}]
  (inline-params env
    {:raw-string raw-string
     :params     (->> params
                   (mapv (fn [param]
                           (or (and (atomic-variable? param)
                                 (get variable-values (:name param)))
                             (single-raw-string param)))))}))

(defn inline-params
  [_env {:keys [raw-string params]}]
  {:params     (into [] (flatten (map :params params)))
   :raw-string (->> (conj (mapv :raw-string params) nil)
                 (interleave (if (= "?" raw-string)
                               ["" ""]
                               (string/split raw-string #"\?")))
                 (apply str))})

(defn concatenate
  [joiner compiled-expressions]
  {:params     (vec (apply concat (map :params compiled-expressions)))
   :raw-string (joiner (map :raw-string compiled-expressions))})

(defn concat-with-and* [xs]
  (string/join " AND "
    (mapv (fn [x] (str "(" x ")")) xs)))

(defn concat-with-and [xs]
  (when (not-empty xs)
    (concatenate concat-with-and* xs)))

(defn concat-with-comma* [xs]
  (when (not-empty xs)
    (string/join ", " xs)))

(def select-all {:raw-string "*" :params []})

(defn concat-with-comma [xs]
  (when (not-empty xs)
    (concatenate concat-with-comma* xs)))

(defn combine-params
  [& compiled-exprs]
  (into [] (comp (map :params) cat)
        compiled-exprs))

(defn compile-to-string
  [env clauses]
  (let [form (s/conform ::expression clauses)]
    (assert (not (s/invalid? form))
            (str "Invalid expression: " clauses))
    (process-expression env form)))

(defn find-variables [{:keys [params]}]
  (into #{} (comp (filter atomic-variable?)
              (map :name))
    params))

(defn selection [compiled-formula clojuric-name]
  (inline-params {}
    {:raw-string "(?) AS ?"
     :params     [compiled-formula (verbatim-raw-string clojuric-name)]}))

(defn build-parameterized-sql-query
  [{:keys [raw-string params]}]
  (vec (cons raw-string params)))
