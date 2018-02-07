(ns walkable.sql-query-builder-test
  (:require [walkable.sql-query-builder :as sut]
            [clojure.test :as t :refer [deftest is]]
            [walkable.sql-query-builder.filters :as filters]
            [com.wsscode.pathom.core :as p]))

(deftest split-keyword-test
  (is (= (sut/split-keyword :foo/bar)
        '("foo" "bar"))))

(deftest keyword->column-name-test
  (is (= (sut/keyword->column-name :foo/bar)
        "foo.bar")))

(deftest keyword->alias-test
  (is (= (sut/keyword->alias :foo/bar)
        "foo/bar")))

(deftest ->column-names-test
  (is (= (sut/->column-names [:foo/bar :loo/lar])
        {:foo/bar "foo.bar", :loo/lar "loo.lar"})))

(deftest ->column-aliases-test
  (is (= (sut/->column-aliases [:foo/bar :loo/lar])
        {:foo/bar "foo/bar", :loo/lar "loo/lar"})))

(deftest selection-with-aliases-test
  (is (= (sut/selection-with-aliases
           [:foo/bar :loo/lar]
           {:foo/bar "foo.bar", :loo/lar "loo.lar"} ;; output of `(->column-names [:foo/bar :loo/lar])`
           {:foo/bar "foo/bar", :loo/lar "loo/lar"} ;; output of `(->column-aliases [:foo/bar :loo/lar])`
           )
        "foo.bar AS \"foo/bar\", loo.lar AS \"loo/lar\"")))

(deftest ->join-statement-test
  (is (= (sut/->join-statement [["foo" "bar"] ["boo" "far"]])
        " JOIN boo ON foo.bar = boo.far")))

(deftest ->join-pairs-tests
  (is (= (sut/->join-pairs [:pet/index :person/number])
        '((("pet" "index")
           ("person" "number")))))
  (is (= (sut/->join-pairs [:pet/index :person-pet/pet-index
                            :person-pet/person-number :person/number])
        '((("pet" "index")
           ("person_pet" "pet_index"))
          (("person_pet" "person_number")
           ("person" "number"))))))

(deftest ->join-statements-tests
  (is (= (sut/->join-statements
           (sut/->join-pairs [:pet/index :person/number]))
        " JOIN person ON pet.index = person.number"))
  (is (= (sut/->join-statements
           (sut/->join-pairs [:pet/index :person-pet/pet-index
                              :person-pet/person-number :person/number]))
        " JOIN person_pet ON pet.index = person_pet.pet_index JOIN person ON person_pet.person_number = person.number")))

(deftest ->source-table-test
  (is (sut/->source-table
        (sut/->join-pairs [:person/number :person-pet/person-number
                           :person-pet/pet-index :pet/index]))
    "person"))

(deftest assoc-multi-tests
  (is (= (sut/assoc-multi {:a 1} [:b :c :d] 2)
        {:a 1, :b 2, :c 2, :d 2}))

  (is (= (sut/assoc-multi {:a 1} [] 2)
        {:a 1}))
  (is (= (sut/assoc-multi {:a 1} :foo 2)
        {:a 1, :foo 2})))

(deftest flatten-multi-keys-test
  (is (= (sut/flatten-multi-keys {:a 1 [:b :c] 2 :d 3})
        {:a 1, :b 2, :c 2, :d 3})))

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

(deftest children->columns-to-query-test
  (is (= (sut/children->columns-to-query
           #::sut{:column-keywords
                  #{:pet/yob}
                  :required-columns
                  {:pet/age #{:pet/yob}}
                  :source-table
                  {:pet/owner "does-not-matter"}
                  :source-column
                  {:pet/owner :person/number}}
           [:pet/age :pet/will-be-ignored :pet/owner])
        #{:pet/yob :person/number})))

(deftest ident->condition-tests
  (is (= (sut/ident->condition
           {:ast {:key [:person/by-id 1]}}
           [:= :person/id])
        {:person/id [:= 1]}))
  (is (= (sut/ident->condition
           {:ast {:key [:people/by-ids 1 2 3]}}
           [:in :person/id])
        {:person/id [:in 1 2 3]})))

(deftest separate-idents-test
  (is (= (sut/separate-idents {:person/by-id  [:= :person/number]
                               :person/by-yob [:= :person/yob]
                               :pets/all      "pet"
                               :people/all    "person"})
        {:unconditional-idents {:pets/all   "pet",
                                :people/all "person"},
         :conditional-idents   #:person {:by-id  [:= :person/number],
                                         :by-yob [:= :person/yob]}})))

(deftest conditional-idents->source-tables-test
  (is (= (sut/conditional-idents->source-tables
           {:person/by-id [:= :person/number]
            :pets/by-ids  [:in :pet/index]})
        {:person/by-id "person", :pets/by-ids "pet"})))

(deftest joins->source-tables-test
  (is (= (sut/joins->source-tables
           {:person/pet [:person/number :person-pet/person-number
                         :person-pet/pet-index :pet/index]
            :pet/owner  [:pet/index :person-pet/pet-index
                         :person-pet/person-number :person/number]})
        {:person/pet "person"
         :pet/owner  "pet"})))

(deftest joins->source-columns-test
  (is (= (sut/joins->source-columns
           {:person/pet [:person/number :person-pet/person-number
                         :person-pet/pet-index :pet/index]
            :pet/owner  [:pet/index :person-pet/pet-index
                         :person-pet/person-number :person/number]})
        {:person/pet :person/number
         :pet/owner  :pet/index})))
