(ns walkable.sql-query-builder.ast-test
  (:require [walkable.sql-query-builder.ast :as sut]
            [com.wsscode.pathom.core :as p]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest find-all-children-test
  (is (= (->>
           (sut/find-all-children
             (first (:children (p/query->ast
                                 '[{:some/root [:x.a/child-col :x.b/child-col
                                                {:ph/ph1 [(:y.a/child-col {:params [1 2]})
                                                          {:y.a/child-join [:y.c/child-col
                                                                            ;; not a direct child
                                                                            {:ph/will-be-ignored [:y.d/child-col]}]}
                                                          {:pp/ph1 [:z.a/child-col :z.b/child-join
                                                                    {:ph/deepest [:z.c/child-col]}]}]}
                                                {:ph/ph2 [:t.a/child-col :t.b/child-col]}]}])))
             {:leaf?        #(contains? #{"child-col" "child-join"}
                               (name (:dispatch-key %)))
              :placeholder? #(contains? #{"ph" "pp"} (namespace (:dispatch-key %)))})
           (map :dispatch-key)
           ;; convert to a set to compare
           (into #{}))
        ;; doesn't contain :y.d/child-col
        #{:x.a/child-col :x.b/child-col
          :y.a/child-col :y.a/child-join
          :z.a/child-col :z.b/child-join :z.c/child-col
          :t.a/child-col :t.b/child-col})))
