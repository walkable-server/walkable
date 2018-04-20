(ns walkable.sql-query-builder-test
  (:require [walkable.sql-query-builder :as sut]
            [clojure.test :as t :refer [deftest is]]
            [fulcro.client.primitives :as prim]
            [com.wsscode.pathom.core :as p]))

(deftest split-keyword-test
  (is (= (sut/split-keyword :foo/bar)
        '("foo" "bar"))))

(deftest keyword->column-name-test
  (is (= (sut/column-name sut/backticks :foo/bar)
        "`foo`.`bar`")))

(deftest clojuric-name-test
  (is (= (sut/clojuric-name sut/backticks :foo/bar)
        "`foo/bar`")))

(deftest ->column-names-test
  (is (= (sut/->column-names sut/backticks [:foo/bar :loo/lar])
        {:foo/bar "`foo`.`bar`", :loo/lar "`loo`.`lar`"})))

(deftest ->clojuric-names-test
  (is (= (sut/->clojuric-names sut/backticks [:foo/bar :loo/lar])
        {:foo/bar "`foo/bar`", :loo/lar "`loo/lar`"})))

(deftest ->join-statement-test
  (is (= (sut/->join-statement {:quote-marks sut/backticks
                                :joins       [["foo" "bar"] ["boo" "far"]]})
        " JOIN `boo` ON `foo`.`bar` = `boo`.`far`")))

(deftest target-column-tests
  (is (= (sut/target-column [:pet/owner :person/number])
        :person/number))
  (is (= (sut/target-column [:pet/index :person-pet/pet-index
                             :person-pet/person-number :person/number])
        :person-pet/pet-index)))

(deftest target-table-tests
  (is (= (sut/target-table [:pet/owner :person/number])
        "person"))
  (is (= (sut/target-table [:pet/index :person-pet/pet-index
                            :person-pet/person-number :person/number])
        "person_pet")))

(deftest ->join-statements-tests
  (is (= (sut/->join-statements sut/backticks
           [:pet/index :person-pet/pet-index
            :person-pet/person-number :person/number])
        " JOIN `person` ON `person_pet`.`person_number` = `person`.`number`")))

(deftest expand-multi-keys-tests
  (is (= (sut/expand-multi-keys {:a 1 [:a :b] 2})
        [[:a 1] [:a 2] [:b 2]])))

(deftest flatten-multi-keys-test
  (is (= (sut/flatten-multi-keys {:a 1 [:b :c] 2 :d 3})
        {:a 1, :b 2, :c 2, :d 3}))
  (is (= (sut/flatten-multi-keys {:a 1 [:a :b] 2 :b 3 :c 4})
        {:a [1, 2] :b [2, 3] :c 4})))

(deftest expand-reversed-joins-test
  (is (= (sut/expand-reversed-joins {:pet/owner :person/pet}
           {:person/pet [:a :b :c :d]})
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

(deftest find-all-children-test
  (is (= (->>
           (sut/find-all-children
             (first (:children (prim/query->ast
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

(deftest process-children-test
  (is (= (sut/process-children
           {:ast {:children (mapv (fn [k] {:dispatch-key k})
                              [:pet/age :pet/will-be-ignored :pet/owner])}
            ::sut/sql-schema
            #::sut{:column-keywords
                   #{:pet/yob}
                   :required-columns
                   {:pet/age #{:pet/yob}}
                   :source-columns
                   {:pet/owner :person/number}}})
        {:join-children #{{:dispatch-key :pet/owner}},
         :columns-to-query #{:pet/yob :person/number}})))

(deftest ident->condition-tests
  (is (= (sut/ident->condition
           {:ast {:key [:person/by-id 1]}}
           :person/id)
        [:= :person/id 1])))

(deftest separate-idents-test
  (is (= (sut/separate-idents {:person/by-id  :person/number
                               :person/by-yob :person/yob
                               :pets/all      "pet"
                               :people/all    "person"})
        {:unconditional-idents {:pets/all   "pet",
                                :people/all "person"},
         :conditional-idents   #:person {:by-id  :person/number,
                                         :by-yob :person/yob}})))

(deftest conditional-idents->target-tables-test
  (is (= (sut/conditional-idents->target-tables
           {:person/by-id :person/number
            :pets/by-ids  :pet/index})
        {:person/by-id "person", :pets/by-ids "pet"})))

(deftest joins->target-tables-test
  (is (= (sut/joins->target-tables
           {:person/pet [:person/number :person-pet/person-number
                         :person-pet/pet-index :pet/index]
            :pet/owner  [:pet/index :person-pet/pet-index
                         :person-pet/person-number :person/number]
            :farmer/cow [:farmer/cow-index :cow/index]})
        {:person/pet "person_pet",
         :pet/owner  "person_pet",
         :farmer/cow "cow"})))

(deftest joins->source-columns-test
  (is (= (sut/joins->source-columns
           {:person/pet [:person/number :person-pet/person-number
                         :person-pet/pet-index :pet/index]
            :pet/owner  [:pet/index :person-pet/pet-index
                         :person-pet/person-number :person/number]
            :farmer/cow [:farmer/cow-index :cow/index]})
        {:person/pet :person/number,
         :pet/owner :pet/index,
         :farmer/cow :farmer/cow-index})))

(deftest joins->target-columns-test
  (is (= (sut/joins->target-columns
           {:person/pet [:person/number :person-pet/person-number
                         :person-pet/pet-index :pet/index]
            :pet/owner  [:pet/index :person-pet/pet-index
                         :person-pet/person-number :person/number]
            :farmer/cow [:farmer/cow-index :cow/index]})
        {:person/pet :person-pet/person-number,
         :pet/owner :person-pet/pet-index,
         :farmer/cow :cow/index})))
