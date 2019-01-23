(ns walkable.sql-query-builder.emitter-test
  (:require [walkable.sql-query-builder.emitter :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest table-name-test
  (is (= (sut/table-name sut/postgres-emitter :prefix.foo/bar)
        "\"prefix\".\"foo\""))
  (is (= (sut/table-name sut/postgres-emitter :foo/bar)
        "\"foo\""))
  (is (= (sut/table-name sut/sqlite-emitter :foo/bar)
        "\"foo\""))
  (is (= (sut/table-name sut/mysql-emitter :foo/bar)
        "`foo`")))

(deftest column-name-test
  (is (= (sut/column-name sut/postgres-emitter :prefix.foo/bar)
        "\"prefix\".\"foo\".\"bar\""))
  (is (= (sut/column-name sut/postgres-emitter :foo/bar)
        "\"foo\".\"bar\""))
  (is (= (sut/column-name sut/sqlite-emitter :foo/bar)
        "\"foo\".\"bar\""))
  (is (= (sut/column-name sut/mysql-emitter :foo/bar)
        "`foo`.`bar`"))
  (is (= (sut/column-name (assoc sut/default-emitter 
                            :transform-column-name identity) 
           :prefix-foo/bar-bar)
        "\"prefix_foo\".\"bar-bar\"")))

(deftest clojuric-name-test
  (is (= (sut/clojuric-name sut/mysql-emitter :foo/bar)
         "`foo/bar`"))
  (is (= (sut/clojuric-name sut/postgres-emitter :prefix.foo/bar)
         "\"prefix.foo/bar\"")))

(deftest emitter->batch-query-test
  (is (= ((sut/emitter->batch-query sut/default-emitter)
          [{:raw-string "x" :params ["a" "b"]}
           {:raw-string "y" :params ["c" "d"]}])
        {:params     ["a" "b" "c" "d"],
         :raw-string "(x)\nUNION ALL\n(y)"}))
  (is (= ((sut/emitter->batch-query sut/sqlite-emitter)
          [{:raw-string "x" :params ["a" "b"]}
           {:raw-string "y" :params ["c" "d"]}])
        {:params     ["a" "b" "c" "d"],
         :raw-string "SELECT * FROM (x)\nUNION ALL\nSELECT * FROM (y)"})))
