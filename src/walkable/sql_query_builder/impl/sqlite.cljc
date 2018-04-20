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

(expressions/def-simple-cast-types {:upper-case? true}
  [:none :real :numeric])

;; http://www.sqlite.org/lang_corefunc.html

(expression/import-functions {:arity 0}
  [random])

(expression/import-functions {:arity 1}
  [abs hex length likely lower quote
   randomblob soundex typeof unicode
   unlikely upper zeroblob])

(expression/import-functions {:aliases '{format "printf"}}
  [char coalesce like likelihood max min glob
   ifnull instr ltrim nullif format replace
   round rtrim substr trim])

;; http://www.sqlite.org/lang_datefunc.html

(expressions/import-functions
  [date time datetime julianday strftime])

;; http://www.sqlite.org/lang_aggfunc.html
(expressions/import-functions {:arity 1}
  [avg count sum total])

;; todo: count(*)

(expressions/import-functions {}
  [group-concat])
