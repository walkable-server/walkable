(ns user
  (:require [clojure.java.io :as io]))

(defn patch-common-edn-files
  "Produce multiple versions of common.edn for each database type.
  You only need to run this if you have modified the file
  `common_unpatched.edn`"
  []
  (let [config-unpatched (slurp (io/resource "common_unpatched.edn"))]
    ;; the sql code in common_unpatched.edn is for mysql
    ;; therefore no patch is needed for it
    (spit "dev/resources/mysql/common.edn" config-unpatched)
    ;; replace all mysql's backticks with quotation marks
    ;; and you get postgres version
    (spit "dev/resources/postgres/common.edn"
      (-> config-unpatched (clojure.string/replace #"`" "\\\\\\\"")))
    ;; sqlite can work with backticks, but it doesn't have
    ;; boolean type, so all `true`s must be replaced with `1`
    ;; and all `false`s with 0.
    (spit "dev/resources/sqlite/common.edn"
      (-> config-unpatched
        (clojure.string/replace #"true" "1")
        (clojure.string/replace #"false" "0")))))

(comment
  (patch-common-edn-files)
)
(defn dev
  "Load and switch to the 'dev' namespace."
  []
  (require 'dev)
  (in-ns 'dev)
  :loaded)
