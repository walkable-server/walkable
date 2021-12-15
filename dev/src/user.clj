(ns user
  (:require [clojure.java.io :as io]
            [clojure.string]))

(defn patch-common-edn-files
  "Produce multiple versions of common.edn for each database type.
  You only need to run this if you have modified the file
  `common_unpatched.edn`"
  []
  (let [config-unpatched (slurp (io/resource "common_unpatched.edn"))]
    ;; the sql code in common_unpatched.edn is for mysql
    ;; therefore no patch is needed for it
    (spit "dev/resources/core-mysql.edn" config-unpatched)
    ;; replace all mysql's backticks with quotation marks
    ;; and you get postgres version
    (spit "dev/resources/core-postgres.edn"
      (-> config-unpatched (clojure.string/replace #"`" "\\\\\\\"")))
    ;; sqlite can work with backticks, but it doesn't have
    ;; boolean type, so all `true`s must be replaced with `1`
    ;; and all `false`s with 0.
    (spit "dev/resources/core-sqlite.edn"
      (-> config-unpatched
        (clojure.string/replace #"true" "1")
        (clojure.string/replace #"false" "0")))))

(comment
  (patch-common-edn-files)
)
(defn dev
  "Load and switch to the 'dev' namespace."
  []
  (require 'dev :reload-all)
  (in-ns 'dev)
  :loaded)
