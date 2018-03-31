(ns walkable.sql-query-builder.filters-test
  (:require [walkable.sql-query-builder.filters :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest is]]))

(deftest parameterize-tuple-tests
  (is (= (sut/parameterize-tuple 0)
        "()"))
  (is (= (sut/parameterize-tuple 3)
        "(?, ?, ?)"))
  (is (= (sut/parameterize-tuple 5)
        "(?, ?, ?, ?, ?)")))

(deftest mask-unsafe-params-test
  (is (= (sut/mask-unsafe-params [:a/b "x" :c/d 2 3 "y"] {:a/b "a.b" :c/d "c.d"})
        {:masked ["a.b" \? "c.d" 2 3 \?], :unmasked ["x" "y"]})))

(deftest conform-conditions-tests
  (is (= (s/conform ::sut/conditions [:nil?])
        [:condition {:operator :nil?, :params [:params []]}]))
  (is (= (s/conform ::sut/conditions [:= 2])
        [:condition {:operator :=, :params [:params [2]]}]))
  (is (= (s/conform ::sut/conditions [:in [1 2]])
        [:condition {:operator :in, :params [:params [1 2]]}]))
  (is (= (s/conform ::sut/conditions [[:= 2] [:> 3]])
        [:conditions {:conditions [[:condition {:operator :=, :params [:params [2]]}]
                                   [:condition {:operator :>, :params [:params [3]]}]]}]))
  (is (= (s/conform ::sut/conditions [:or [:= 2] [:> 3]])
        [:conditions {:combinator :or,
                      :conditions [[:condition {:operator :=, :params [:params [2]]}]
                                   [:condition {:operator :>, :params [:params [3]]}]]}]))
  (is (= (s/conform ::sut/conditions [[[[:and [:= 2] [:> 3]]]]])
        [:conditions
         {:conditions
          [[:conditions
            {:conditions
             [[:conditions
               {:conditions
                [[:conditions
                  {:combinator :and,
                   :conditions [[:condition {:operator :=,
                                             :params [:params [2]]}]
                                [:condition {:operator :>,
                                             :params [:params [3]]}]]}]]}]]}]]}]))
  (is (= (s/conform ::sut/conditions [:and [:> 3] [:or [:= 5] [:= -2]]])
        [:conditions {:combinator :and,
                      :conditions [[:condition {:operator :>,
                                                :params [:params [3]]}]
                                   [:conditions {:combinator :or,
                                                 :conditions [[:condition {:operator :=,
                                                                           :params [:params [5]]}]
                                                              [:condition {:operator :=,
                                                                           :params [:params [-2]]}]]}]]}])))

(deftest process-clauses-tests
  (is (= (sut/process-clauses
           {:key    nil
            :keymap {:p/a "p.a"
                     :p/b "p.b"
                     :p/c "p.c"}}
           (s/conform ::sut/clauses {:p/c [:= 7]
                                     :p/b [:in #{6 4 7 2}]
                                     :p/a [:> 5]}))
        '("("
          {:condition "p.c = ?", :params [7]}
          " AND "
          {:condition "p.b IN (?, ?, ?, ?)", :params #{7 4 6 2}}
          " AND "
          {:condition "p.a > ?", :params [5]} ")"))))

(deftest parameterize-tests
  (is (= (sut/parameterize
           {:key    nil
            :keymap {:person/number "p.n"}}
           [#:person{:number [:= 2]}])
        '["p.n = ?" (2)]))
  (is (= (sut/parameterize
           {:key    nil
            :keymap {:person/number "p.n"}}
           [:or [#:person{:number [:= 2]}]
            [#:person{:number [:= 3]}]])
        '["(p.n = ? OR p.n = ?)" (2 3)]))
  (is (= (sut/parameterize
           {:key    nil
            :keymap {:p/a "p.a"
                     :p/b "p.b"
                     :p/c "p.c"}}
           [:or
            [{:p/a [:= 9] :p/b [:= 2]}
             {:p/c [:= 7]}]
            [{:p/a [:= 11] :p/b [[[:= 222]]]}
             {:p/c [:or  [:= -3] [:= -2]]}]])
        ["(((p.a = ? AND p.b = ?) AND p.c = ?) OR ((p.a = ? AND p.b = ?) AND (p.c = ? OR p.c = ?)))"
         '(9 2 7 11 222 -3 -2)])))

(deftest ->order-by-string-tests
  (is (= (sut/->order-by-string {:person/name "p.n" :person/age "p.a"}
           [:person/name
            :person/age :desc :nils-last])
        "p.n, p.a DESC NULLS LAST"))

  (is (nil? (sut/->order-by-string {:person/name "p.n" :person/age "p.a"}
              [])))

  (is (nil? (sut/->order-by-string {}
              [:person/name [:person/age :desc]]))))
