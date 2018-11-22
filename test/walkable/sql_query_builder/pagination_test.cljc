(ns walkable.sql-query-builder.pagination-test
  (:require [walkable.sql-query-builder.pagination :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest ->order-by-string-tests
  (is (= (sut/->order-by-string {:person/name "`p`.`n`" :person/age "`p`.`a`"}
           [{:column :person/name} {:column :person/age, :params [:desc :nils-last]}])
        "`p`.`n`, `p`.`a` DESC NULLS LAST"))

  (is (nil? (sut/->order-by-string {:person/name "`p`.`n`" :person/age "`p`.`a`"}
              nil))))

(deftest wrap-validate-number-test
  (is (= (->> (range 8) (map (sut/wrap-validate-number #(<= 2 % 4))))
        [false false true true true false false false]))
  (is (= (->> [:invalid 'types] (map (sut/wrap-validate-number #(<= 2 % 4))))
        [false false])))

(deftest conform-order-by-test
  (is (= (map #(sut/conform-order-by {:x/a "`x/a`" :x/b "`x/b`"} %)
           [[:x/a :asc :x/b :desc :nils-first :x/invalid-key]
            [:x/a :asc :x/b :desc :nils-first 'invalid-type]
            [:x/a :asc :x/b :desc :nils-first]
            :invalid-type])
        [[{:column :x/a, :params [:asc]} {:column :x/b, :params [:desc :nils-first]}]
         nil
         [{:column :x/a, :params [:asc]} {:column :x/b, :params [:desc :nils-first]}]
         nil])))

(deftest wrap-validate-order-by-test
  (is (= (map (sut/wrap-validate-order-by #{:x/a :x/b})
           [[{:column :x/a, :params [:asc]} {:column :x/b, :params [:desc :nils-first]}]
            [{:column :x/a, :params [:asc]} {:column :x/invalid-key, :params [:desc :nils-first]}]
            nil])
        [true false false]))
  (is (= (map (sut/wrap-validate-order-by nil)
           [[{:column :x/a, :params [:asc]} {:column :x/b, :params [:desc :nils-first]}]
            [{:column :x/a, :params [:asc]} {:column :x/any-key, :params [:desc :nils-first]}]
            nil])
        [true true false])))

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
