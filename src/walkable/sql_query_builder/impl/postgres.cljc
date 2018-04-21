(ns walkable.sql-query-builder.impl.postgres
  (:require [walkable.sql-query-builder.expressions :as expressions]))

;; https://www.postgresql.org/docs/current/static/functions-array.html

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

(expressions/import-infix-operators {}
  {concat "||"})

;; https://www.postgresql.org/docs/current/static/functions-json.html

;; instead of "?", "?|"
;; source: https://stackoverflow.com/questions/30629076/how-to-escape-the-question-mark-operator-to-query-postgresql-jsonb-type-in-r
(expressions/import-functions {}
  [jsonb-exists jsonb-exists-any])

(expressions/import-infix-operators {:arity 2}
  {get            "->"
   get-as-text    "->>"
   get-in         "#>"
   get-in-as-text "#>>"})
