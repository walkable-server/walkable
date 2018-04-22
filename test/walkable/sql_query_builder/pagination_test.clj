(ns walkable.sql-query-builder.pagination-test
  (:require [walkable.sql-query-builder.pagination :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest ->order-by-string-tests
  (is (= (sut/->order-by-string {:person/name "p.n" :person/age "p.a"}
           [:person/name
            :person/age :desc :nils-last])
        "p.n, p.a DESC NULLS LAST"))

  (is (nil? (sut/->order-by-string {:person/name "p.n" :person/age "p.a"}
              [])))

  (is (nil? (sut/->order-by-string {}
              [:person/name [:person/age :desc]]))))
