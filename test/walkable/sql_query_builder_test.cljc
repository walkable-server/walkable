(ns walkable.sql-query-builder-test
  (:require [walkable.sql-query-builder :as sut]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [clojure.test :as t :refer [deftest is]]
            [com.wsscode.pathom.core :as p]))

(deftest process-children-test
  (is (= (sut/process-children
           {:ast             {:children (mapv (fn [k] {:dispatch-key k})
                                          [:pet/age :pet/will-be-ignored :pet/owner])}
            ::sut/floor-plan {::floor-plan/column-keywords
                              #{:pet/yob}
                              ::floor-plan/required-columns
                              {:pet/age #{:pet/yob}}
                              ::floor-plan/source-columns
                              {:pet/owner :person/number}}})
        {:join-children    #{{:dispatch-key :pet/owner}},
         :columns-to-query #{:pet/yob :person/number}})))

(deftest evaluate-formulas-test
  (is (= (sut/evaluate-formulas
           {::sut/floor-plan {::floor-plan/join-filter-subqueries {:j/m "jm"}
                              ::floor-plan/true-columns           {:x/a "x.a"}
                              ::floor-plan/stateless-formulas     {:x/b {:raw-string "x.a" :params []}}
                              ::floor-plan/stateful-formulas      {:x/c (fn [env]
                                                                          (let [year (-> env :current/year)]
                                                                            {:raw-string "? - x.a" :params [year]}))}}
            :current/year    2018}
          #{:x/a :x/b :x/c})
        {:true-columns           #:x {:a "x.a"},
         :formulas               #:x {:c {:raw-string "? - x.a", :params [2018]},
                                      :b {:raw-string "x.a", :params []}},
         :join-filter-subqueries #:j {:m "jm"}})))

(deftest ident->condition-tests
  (is (= (sut/ident->condition
           {:ast {:key [:person/by-id 1]}}
           :person/id)
        [:= :person/id 1])))
