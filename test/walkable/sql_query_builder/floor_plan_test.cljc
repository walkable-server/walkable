(ns walkable.sql-query-builder.floor-plan-test
  (:require [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.floor-plan :as sut]
            [walkable.sql-query-builder.pagination :as pagination]
            [walkable.sql-query-builder.expressions :as expressions]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest column-names-test
  (is (= (sut/column-names emitter/mysql-emitter [:foo/bar :loo/lar])
        {:foo/bar "`foo`.`bar`", :loo/lar "`loo`.`lar`"})))

(deftest clojuric-names-test
  (is (= (sut/clojuric-names emitter/mysql-emitter [:foo/bar :loo/lar])
        {:foo/bar "`foo/bar`", :loo/lar "`loo/lar`"})))

(deftest target-column-tests
  (is (= (sut/target-column [:pet/owner :person/number])
        :person/number))
  (is (= (sut/target-column [:pet/index :person-pet/pet-index
                             :person-pet/person-number :person/number])
        :person-pet/pet-index)))

(deftest target-table-tests
  (is (= (sut/target-table emitter/mysql-emitter [:pet/owner :person/number])
        "`person`"))
  (is (= (sut/target-table emitter/mysql-emitter
           [:pet/index :person-pet/pet-index
            :person-pet/person-number :person/number])
        "`person_pet`")))

(deftest join-statements-tests
  (is (nil? (sut/join-statements emitter/mysql-emitter [:pet/owner-id :person/id])))
  (is (= (sut/join-statements emitter/mysql-emitter
           [:pet/index :person-pet/pet-index
            :person-pet/person-number :person/number])
        " JOIN `person` ON `person_pet`.`person_number` = `person`.`number`")))

(deftest joins->target-tables-test
  (is (= (sut/joins->target-tables emitter/mysql-emitter
           {:person/pet [:person/number :person-pet/person-number
                         :person-pet/pet-index :pet/index]
            :pet/owner  [:pet/index :person-pet/pet-index
                         :person-pet/person-number :person/number]
            :farmer/house [:farmer/house-index :house/index]})
         {:person/pet "`person_pet`",
          :pet/owner  "`person_pet`",
          :farmer/house "`house`"})))

(deftest joins->source-columns-test
  (is (= (sut/joins->source-columns
           {:person/pet [:person/number :person-pet/person-number
                         :person-pet/pet-index :pet/index]
            :pet/owner  [:pet/index :person-pet/pet-index
                         :person-pet/person-number :person/number]
            :farmer/house [:farmer/house-index :house/index]})
        {:person/pet :person/number,
         :pet/owner :pet/index,
         :farmer/house :farmer/house-index})))

(deftest joins->target-columns-test
  (is (= (sut/joins->target-columns
           {:person/pet [:person/number :person-pet/person-number
                         :person-pet/pet-index :pet/index]
            :pet/owner  [:pet/index :person-pet/pet-index
                         :person-pet/person-number :person/number]
            :farmer/house [:farmer/house-index :house/index]})
        {:person/pet :person-pet/person-number,
         :pet/owner :person-pet/pet-index,
         :farmer/house :house/index})))

(deftest expand-multi-keys-tests
  (is (= (sut/expand-multi-keys {:a 1 [:a :b] 2})
        [[:a 1] [:a 2] [:b 2]])))

(deftest flatten-multi-keys-test
  (is (= (sut/flatten-multi-keys {:a 1 [:b :c] 2 :d 3})
        {:a 1, :b 2, :c 2, :d 3}))
  (is (= (sut/flatten-multi-keys {:a 1 [:a :b] 2 :b 3 :c 4})
        {:a [1, 2] :b [2, 3] :c 4})))

(deftest expand-reversed-joins-test
  (is (= (sut/expand-reversed-joins
           {:person/pet [:a :b :c :d]}
           {:pet/owner :person/pet})
        {:person/pet [:a :b :c :d], :pet/owner [:d :c :b :a]})))

(deftest expand-denpendencies*-test
  (is (= (sut/expand-denpendencies* {:a #{:b :c}
                                     :b #{:d}
                                     :d #{:e}})
        {:a #{:c :d}, :b #{:e}, :d #{:e}})))

(deftest expand-denpendencies-test
  (is (= (sut/expand-denpendencies {:a #{:b :c}
                                    :b #{:d}
                                    :d #{:e}})
        {:a #{:e :c}, :b #{:e}, :d #{:e}})))

(deftest idents->target-tables-test
  (is (= (sut/idents->target-tables emitter/mysql-emitter
           #{:person/number :pet/index})
        {:person/number "`person`", :pet/index "`pet`"})))

(deftest roots->target-tables-test
  (is (= (sut/roots->target-tables emitter/mysql-emitter
           {:people/people "person"
            :pets/pets   "pet"})
        {:people/people "`person`", :pets/pets "`pet`"}))

  (is (= (sut/roots->target-tables emitter/default-emitter
           {:people/people "public.person"
            :pets/pets   "public.pet"})
        {:people/people "\"public\".\"person\""
         :pets/pets "\"public\".\"pet\""})))

(deftest unbound-expression?-test
  (is (false? (sut/unbound-expression? {:raw-string "abc"
                                        :params     []})))
  (is (false? (sut/unbound-expression? {:raw-string "abc AND ?"
                                        :params     ["bla"]})))
  (is (true? (sut/unbound-expression? {:raw-string "abc AND ?"
                                       :params     [(expressions/av :x/a)]}))))

(deftest rotate-test
  (is (= (sut/rotate [:a :b :c :d]) [:b :c :d :a]))
  (is (= (sut/rotate [:a :b]) [:b :a]))
  (is (= (sut/rotate []) [])))

(deftest compile-formulas-once-test
  (is (= (sut/compile-formulas-once
          (sut/compile-true-columns emitter/postgres-emitter
            #{:x/a :x/b})
          {:x/c 99
           :x/d [:- 100 :x/c]})
        {:unbound #:x {:d {:params     [(expressions/av :x/c)],
                           :raw-string "(100)-(?)"}},
         :bound   #:x {:a {:raw-string "\"x\".\"a\"", :params []},
                       :b {:raw-string "\"x\".\"b\"", :params []},
                       :c {:raw-string "99", :params []}}})))

(deftest compile-formulas-recursively-test
  (is (= (sut/compile-formulas-recursively
          (sut/compile-formulas-once
            (sut/compile-true-columns
              emitter/postgres-emitter #{:x/a :x/b})
            {:x/c 99
             :x/d [:- 100 :x/c]}))
        {:unbound {},
         :bound   #:x {:a {:raw-string "\"x\".\"a\"", :params []},
                       :b {:raw-string "\"x\".\"b\"", :params []},
                       :c {:raw-string "99", :params []},
                       :d {:raw-string "(100)-(99)", :params []}}})))

(deftest column-dependencies-test
  (is (= (sut/column-dependencies
          (sut/compile-formulas-recursively
            (sut/compile-formulas-once
              (sut/compile-true-columns
                emitter/postgres-emitter #{:x/a :x/b})
              {:x/c [:+ :x/d (expressions/av 'o)]
               :x/d [:- 100 :x/e]
               :x/e [:- 100 :x/c]})))
        #:x{:d #{:x/e}, :e #{:x/c}, :c #{:x/d}})))

(deftest compile-selection-test
  (is (= (sut/compile-selection
           {:raw-string "? - `human`.`yob` ?"
            :params     [(expressions/av 'current-year)]}
          "`human/age`")
        {:params     [(expressions/av 'current-year)],
         :raw-string "(? - `human`.`yob` ?) AS `human/age`"})))

(deftest columns-in-joins-test
  (is (= (sut/columns-in-joins {:x [:u :v] :y [:m :n]})
       #{:v :n :m :u})))

(deftest compile-cte-keywords-test
  (is (= (-> (sut/compile-cte-keywords {:joins   {:x/a [] :x/b [] :x/c []}
                                        :use-cte {:x/b false :default true}})
           (select-keys [:cte-keywords]))
        {:cte-keywords #{:x/a :x/c}}))
  (is (= (-> (sut/compile-cte-keywords {:joins  {:x/a [] :x/b [] :x/c []}
                                        :use-cte {:x/a true :x/b true :default false}})
           (select-keys [:cte-keywords]))
        {:cte-keywords #{:x/a :x/b}})))

(deftest polulate-columns-with-joins-test
  (is (= (sut/polulate-columns-with-joins {:joins        {:x [:u :v] :y [:m :n]}
                                           :true-columns #{:a :b}})
        {:joins        {:x [:u :v], :y [:m :n]},
         :true-columns #{:v :n :m :b :a :u}})))

(deftest member->graph-id-test
  (is (= (let [graphs [{:graph (zipmap [:xs :n :m :m2 :v]
                                 (repeat :just-a-function))}
                       {:graph (zipmap [:p :q]
                                 (repeat :just-a-function))}]]
           (sut/member->graph-id graphs))
        '{xs 0, n 0, m 0, m2 0, v 0, p 1, q 1})))
