(ns walkable.sql-query-builder.helper-test
  (:require [walkable.sql-query-builder.helper :as sut]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest build-index-tests
  (is (= (sut/build-index :key [{:key :foo :a 1}
                                {:key :bar :b 2}])
        {:foo {:key :foo, :a 1}
         :bar {:key :bar, :b 2}})))

(deftest build-index-of-tests
  (is (= (sut/build-index-of :a [{:key :foo :a 1}
                                 {:key :bar :a 2}])
        {:foo 1, :bar 2})))
