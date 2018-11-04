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
        "`foo`.`bar`")))

(deftest clojuric-name-test
  (is (= (sut/clojuric-name sut/mysql-emitter :foo/bar)
         "`foo/bar`"))
  (is (= (sut/clojuric-name sut/postgres-emitter :prefix.foo/bar)
         "\"prefix.foo/bar\"")))
