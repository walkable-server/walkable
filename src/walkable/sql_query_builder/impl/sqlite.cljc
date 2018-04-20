(ns walkable.sql-query-builder.impl.sqlite
  (:require [walkable.sql-query-builder.expressions :as expressions]))

(defmethod expressions/process-expression :boolean
  [_env [_kw value]]
  {:raw-string " ? "
   :params     [value]})

(defmethod expressions/process-operator :str
  [_env [_operator params]]
  (if (empty? params)
    {:raw-string "''"
     :params     []}
    {:raw-string (clojure.string/join "||"
                   (repeat (count params) "COALESCE(?, '')"))
     :params     params}))

(defmethod expressions/operator? :format [_operator] false)
