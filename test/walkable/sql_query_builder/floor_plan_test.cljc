(ns walkable.sql-query-builder.floor-plan-test
  (:require [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.floor-plan :as sut]
            [walkable.sql-query-builder.pagination :as pagination]
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
            :farmer/cow [:farmer/cow-index :cow/index]})
         {:person/pet "`person_pet`",
          :pet/owner  "`person_pet`",
          :farmer/cow "`cow`"})))

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
  (is (= (sut/conditional-idents->target-tables emitter/mysql-emitter
           {:person/by-id :person/number
            :pets/by-ids  :pet/index})
         {:person/by-id "`person`", :pets/by-ids "`pet`"})))

(deftest unconditional-idents->target-tables-test
  (is (= (sut/unconditional-idents->target-tables emitter/mysql-emitter
           {:people/all "person"
            :pets/all   "pet"})
        {:people/all "`person`", :pets/all "`pet`"}))

  (is (= (sut/unconditional-idents->target-tables emitter/default-emitter
           {:people/all "public.person"
            :pets/all   "public.pet"})
        {:people/all "\"public\".\"person\""
         :pets/all "\"public\".\"pet\""})))

(deftest merge-pagination-test
  (let [all-fallbacks     (sut/compile-pagination-fallbacks
                            {:x/a "`x/a`" :x/b "`x/b`" :x/random-key "`x/random-key`"}
                            {:people/all
                             {:offset   {:default  5
                                         :validate #(<= 2 % 4)}
                              :limit    {:default  10
                                         :validate #(<= 12 % 14)}
                              :order-by {:default  [:x/a]
                                         :validate #{:x/a :x/b}}}})
        current-fallbacks (:people/all all-fallbacks)]
    (is (= (pagination/merge-pagination
             nil
             {:offset             4
              :limit              8
              :conformed-order-by [{:column :x/b}]})
          {:offset             4,
           :limit              8,
           :conformed-order-by [{:column :x/b}]}))
    (is (= (pagination/merge-pagination
             current-fallbacks
             {:offset             4
              :limit              8
              :conformed-order-by [:x/invalid-key]})
          {:offset             4,
           :limit              10,
           :conformed-order-by [{:column :x/a}]}))
    (is (= (pagination/merge-pagination
             current-fallbacks
             {:offset             4
              :limit              :invalid-type
              :conformed-order-by [{:column :x/a}]})
          {:offset             4,
           :limit              10,
           :conformed-order-by [{:column :x/a}]}))))

(deftest merge-pagination-partially-test
  (let [all-fallbacks     (sut/compile-pagination-fallbacks
                            {:x/a "`x/a`" :x/b "`x/b`" :x/random-key "`x/random-key`"}
                            {:people/all
                             {:offset {:default  5
                                       :validate #(<= 2 % 4)}}})
        current-fallbacks (:people/all all-fallbacks)]
    (is (= (pagination/merge-pagination
             current-fallbacks
             {:offset             4
              :limit              8
              :conformed-order-by [{:column :x/a}]})
          {:offset             4
           :limit              8
           :conformed-order-by [{:column :x/a}]}))
    (is (= (pagination/merge-pagination
             current-fallbacks
             {:offset             6
              :limit              8
              :conformed-order-by [{:column :x/random-key}]})
          {:offset             5
           :limit              8
           :conformed-order-by [{:column :x/random-key}]}))))
