(ns walkable.sql-query-builder-test
  (:require [walkable.sql-query-builder :as sut]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [clojure.test :as t :refer [deftest is]]
            [com.wsscode.pathom.core :as p]))

(deftest process-children-test
  (is (= (sut/process-children
           {:ast          {:children (mapv (fn [k] {:dispatch-key k})
                                       [:pet/age :pet/will-be-ignored :pet/owner])}
            ::sut/floor-plan
            #::floor-plan {:column-keywords
                           #{:pet/yob}
                           :required-columns
                           {:pet/age #{:pet/yob}}
                           :source-columns
                           {:pet/owner :person/number}}})
        {:join-children    #{{:dispatch-key :pet/owner}},
         :columns-to-query #{:pet/yob :person/number}})))

(deftest ident->condition-tests
  (is (= (sut/ident->condition
           {:ast {:key [:person/by-id 1]}}
           :person/id)
        [:= :person/id 1])))

(deftest merge-pagination-test
  (is (= (sut/merge-pagination {:offset 5 'limit 5 :order-by :e} {:offset 10 :limit 10 :order-by :s})
        {:offset 5, :limit 10, :order-by :e}))
  (is (= (sut/merge-pagination {:offset nil 'limit 5 :order-by :e} {:offset 10 :limit 10 :order-by :s})
        {:offset nil, :limit 10, :order-by :e})))
