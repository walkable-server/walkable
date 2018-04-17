(ns walkable.sql-query-builder.filters-test
  (:require [walkable.sql-query-builder.filters :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest inline-params-tests
  (is (= (sut/inline-params
           {:raw-string " ? "
            :params     [{:raw-string "2018 - `human`.`yob`"
                          :params     []}]})
        {:raw-string " 2018 - `human`.`yob` "
         :params     []})))

