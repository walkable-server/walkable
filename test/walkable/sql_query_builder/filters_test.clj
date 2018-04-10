(ns walkable.sql-query-builder.filters-test
  (:require [walkable.sql-query-builder.filters :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is]]))

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

(deftest inline-safe-params-test
  (is (= (sut/inline-safe-params
         {:raw-string   "a = ? AND b = ? AND c = ? OR d > ?"
          :params       [:a/b "d" :c/d 2]
          :column-names {:a/b "a.b" :c/d "c.d"}})
      {:params ["d"], :raw-string "a = a.b AND b = ? AND c = c.d OR d > 2"})))

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

(deftest raw-string+params-test
  (is (= (with-redefs [sut/parameterize-operator (fn [_operator column params]
                                                   (str "? AND ? " column " ?"))]
           (sut/raw-string+params :mock "`a`.`b`" [1 "a" 2]))
        {:raw-string "? AND ? `a`.`b` ?", :params [1 "a" 2]}))
  (is (= (with-redefs [sut/parameterize-operator (fn [_operator column params]
                                                   (str "? AND ? " column " ?"))]
           (sut/raw-string+params :mock
             ["CONCAT(?, ?, ?)" :a/b "x" 99]
             [1 "y" 2]))
        {:raw-string "? AND ? CONCAT(?, ?, ?) ?", :params [1 "y" :a/b "x" 99 2]})))

(deftest process-clauses-tests
  (is (= (sut/process-clauses
           {:key    nil
            :keymap {:p/a "p.a"
                     :p/b "p.b"
                     :p/c "p.c"}}
           (s/conform ::sut/clauses {:p/c [:= 7]
                                     :p/b [:in #{6 "a" 7 "b"}]
                                     :p/a [:> 5]}))
        '("("
          {:condition "p.c = 7", :params []}
          " AND "
          {:condition "p.b IN (7, 6, ?, ?)", :params ["a" "b"]}
          " AND "
          {:condition "p.a > 5", :params []} ")")))
  (is (= (sut/process-clauses
           {:key    nil
            :keymap {:p/a "p.a"
                     :p/b "p.b"
                     :t/x "t.x"
                     :t/y "t.y"}
            :join-strings {:p/t "(p.id IN (SELECT p_t.pid FROM p_t JOIN t ON p_t.tid = t.id WHERE "}}
           (s/conform ::sut/clauses {:p/b [:in #{6 "a" 7 "b"}]
                                     :p/a [:> 5]
                                     :p/t {:t/x [:= 2]
                                           :t/y [:like "yo"]}}))
        '("(" {:params ["a" "b"],
               :condition "p.b IN (7, 6, ?, ?)"}
          " AND " {:params [], :condition "p.a > 5"}
          " AND " ["(p.id IN (SELECT p_t.pid FROM p_t JOIN t ON p_t.tid = t.id WHERE "
                   (("(" {:params [], :condition "t.x = 2"}
                     " AND " {:params ["yo"], :condition "t.y LIKE ?"}
                     ")"))
                   "))"]
          ")")
        )))

(deftest parameterize-tests
  (testing "parameterize with unsafe params"
   (is (= (sut/parameterize
             {:key    nil
              :keymap {:person/number "p.n"}}
             [#:person{:number [:= "a"]}])
          ["p.n = ?" ["a"]])))
  (testing "parameterize with safe params"
    (is (= (sut/parameterize
             {:key    nil
              :keymap {:person/number "p.n"}}
             [#:person{:number [:= 2]}])
          ["p.n = 2" []]))
    (is (= (sut/parameterize
             {:key    nil
              :keymap {:person/number "p.n"
                       :person/value  "p.v"}}
             [#:person{:number [:= :person/value]}])
          ["p.n = p.v" []]))
    (is (= (sut/parameterize
             {:key    nil
              :keymap {:person/number "p.n"}}
             [:or [#:person{:number [:= 2]}]
              [#:person{:number [:= 3]}]])
          ["(p.n = 2 OR p.n = 3)" []])))
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
        ["(((p.a = 9 AND p.b = 2) AND p.c = 7) OR ((p.a = 11 AND p.b = 222) AND (p.c = -3 OR p.c = -2)))"
         []])))

(deftest ->order-by-string-tests
  (is (= (sut/->order-by-string {:person/name "p.n" :person/age "p.a"}
           [:person/name
            :person/age :desc :nils-last])
        "p.n, p.a DESC NULLS LAST"))

  (is (nil? (sut/->order-by-string {:person/name "p.n" :person/age "p.a"}
              [])))

  (is (nil? (sut/->order-by-string {}
              [:person/name [:person/age :desc]]))))
