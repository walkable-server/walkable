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

(deftest wrap-validate-number-test
  (is (= (->> (range 8) (map (sut/wrap-validate-number #(<= 2 % 4))))
        [false false true true true false false false]))
  (is (= (->> [:invalid 'types] (map (sut/wrap-validate-number #(<= 2 % 4))))
        [false false])))

(deftest order-by-columns-test
  (is (= (sut/order-by-columns [:x/a :asc :x/b :desc :nils-first :x/c])
        [:x/a :x/b :x/c]))
  (is (= (sut/order-by-columns [:x/a :asc :x/b :desc :nils-first :y])
        nil))
  (is (= (sut/order-by-columns [:x/a :asc :x/b :desc :nils-first 0])
        nil)))

(deftest wrap-validate-order-by-test
  (is (= (map (sut/wrap-validate-order-by #{:x/a :x/b})
           [[:x/a :asc :x/b :desc :nils-first]
            [:x/a :asc :x/b :desc :nils-first :x/invalid-key]
            [:x/a :asc :x/b :desc :nils-first :not-namespaced-keyword]
            [:x/a :asc :x/b :desc :nils-first 'invalid-type]
            :invalid-type])
        [true false false false false])))

