(ns walkable.sql-query-builder.impl.sqlite
  (:require [walkable.sql-query-builder.expressions :as expressions]))

(extend-protocol expressions/EmittableAtom
  #?(:clj Boolean :cljs boolean)
  (emit [boolean-val]
    (expressions/single-raw-string boolean-val)))

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

;; http://www.sqlite.org/lang_expr.html
;; http://www.sqlite.org/lang_corefunc.html

(expressions/import-functions {:arity 0}
  [random])

(expressions/import-functions {:arity 1}
  [abs hex length likely lower quote
   randomblob soundex typeof unicode
   unlikely upper zeroblob])

(expressions/import-functions {}
  [char coalesce like likelihood max min glob
   ifnull instr ltrim nullif printf replace
   round rtrim substr trim])

(expressions/import-functions {}
  {format "printf"})

;; http://www.sqlite.org/lang_datefunc.html

(expressions/import-functions {}
  [date time datetime julianday strftime])

;; http://www.sqlite.org/lang_aggfunc.html
(expressions/import-functions {:arity 1}
  [avg count sum total])

;; todo: count(*)

(expressions/import-functions {}
  [group-concat])
