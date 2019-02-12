(ns walkable.sql-query-builder.impl.postgres
  (:require #?(:clj [cheshire.core :refer [generate-string]])
            [walkable.sql-query-builder.expressions :as expressions]))

(extend-protocol expressions/EmittableAtom
  #?(:clj java.util.UUID :cljs UUID)
  (emit [uuid]
    (expressions/single-raw-string uuid)))

;; https://www.postgresql.org/docs/current/static/functions-array.html

(defmethod expressions/operator? :array [_operator] true)

(defmethod expressions/process-operator :array
  [_env [_operator params]]
  {:raw-string (str "ARRAY[" (clojure.string/join ", "
                               (repeat (count params) "?"))
                 "]")
   :params     params})

(expressions/import-functions {}
  [array-append array-cat
   array-fill
   array-length array-lower array-position array-positions
   array-prepend array-remove array-replace array-to-string
   array-upper cardinality string-to-array unnest])

(expressions/import-functions {:arity 1}
  [array-ndims array-dims])

(expressions/import-infix-operators {:arity 2}
  {contains "@>"
   overlap   "&&"})

(expressions/def-simple-cast-types {:upper-case? true}
  [:json :jsonb])

(expressions/import-infix-operators {}
  {concat "||"})

;; https://www.postgresql.org/docs/current/static/functions-json.html

#?(:cljs
   (defn generate-string
     "Equivalent of cheshire.core/generate-string for Clojurescript"
     [ds]
     (.stringify js/JSON (clj->js ds))))

(defmethod expressions/unsafe-expression? :json* [_] true)

(defmethod expressions/process-unsafe-expression :json*
  [_env [_operator [json]]]
  (let [json-string (generate-string json)]
    {:raw-string "?"
     :params     [json-string]}))

(defmethod expressions/unsafe-expression? :json [_] true)

(defmethod expressions/process-unsafe-expression :json
  [_env [_operator [json]]]
  (let [json-string (generate-string json)]
    {:raw-string "?::json"
     :params     [json-string]}))

(defmethod expressions/unsafe-expression? :jsonb [_] true)

(defmethod expressions/process-unsafe-expression :jsonb
  [_env [_operator [json]]]
  (let [json-string (generate-string json)]
    {:raw-string "?::jsonb"
     :params     [json-string]}))

;; instead of "?", "?|"
;; source: https://stackoverflow.com/questions/30629076/how-to-escape-the-question-mark-operator-to-query-postgresql-jsonb-type-in-r
(expressions/import-functions {}
  [to-char to-date to-number to-timestamp iso-timestamp json-agg
   jsonb-contains jsonb-exists jsonb-exists-any jsonb-exists-all jsonb-delete-path])

(expressions/import-infix-operators {:arity 2}
  {get            "->"
   get-as-text    "->>"
   get-in         "#>"
   get-in-as-text "#>>"})
