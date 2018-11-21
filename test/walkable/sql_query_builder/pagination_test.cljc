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

(deftest offset-fallback-test
  (is (= (map (sut/offset-fallback {:default 2 :validate #(<= 2 % 4)})
           (range 8))
        [2 2 2 3 4 2 2 2]))
  (is (= (map (sut/offset-fallback {:default 2 :validate #(<= 2 % 4)})
           [:invalid 'types])
        [2 2])))

(deftest order-by-fallback-test
  (is (= (map (sut/order-by-fallback {:default [:x/a] :validate #{:x/a :x/b}})
           [[:x/a :asc :x/b :desc :nils-first]
            [:x/a :asc :x/invalid-key :desc :nils-first]
            [:x/a :asc :x/b :desc :nils-first :not-namespaced-keyword]
            [:x/a :asc :x/b :desc :nils-first 'invalid-type]])
        [[:x/a :asc :x/b :desc :nils-first]
         [:x/a]
         [:x/a]
         [:x/a]])))
