(ns walkable.sql-query-builder-test
  (:require [walkable.sql-query-builder :as sut]
            [clojure.test :as t :refer [deftest is]]
            [walkable.sql-query-builder.filters :as filters]
            [com.wsscode.pathom.core :as p]))

(deftest split-keyword-test
  (is (= (sut/split-keyword :foo/bar)
        '("foo" "bar"))))

(deftest keyword->column-name-test
  (is (= (sut/column-name :foo/bar)
        "`foo`.`bar`")))

(deftest clojuric-name-test
  (is (= (sut/clojuric-name :foo/bar)
        "`foo/bar`")))

(deftest ->column-names-test
  (is (= (sut/->column-names [:foo/bar :loo/lar])
        {:foo/bar "`foo`.`bar`", :loo/lar "`loo`.`lar`"})))

(deftest ->clojuric-names-test
  (is (= (sut/->clojuric-names [:foo/bar :loo/lar])
        {:foo/bar "`foo/bar`", :loo/lar "`loo/lar`"})))

(deftest selection-with-aliases-test
  (is (= (sut/selection-with-aliases
           {:columns-to-query [:foo/bar :loo/lar]
            :column-names     {:foo/bar "`foo`.`bar`"
                               :loo/lar "`loo`.`lar`"}
            :clojuric-names   {:foo/bar "`foo/bar`",
                               :loo/lar "`loo/lar`",}})
        "`foo`.`bar` AS `foo/bar`, `loo`.`lar` AS `loo/lar`")))

(deftest ->join-statement-test
  (is (= (sut/->join-statement [["foo" "bar"] ["boo" "far"]])
        " JOIN `boo` ON `foo`.`bar` = `boo`.`far`")))

(deftest ->join-tables-test
  (is (= (sut/->join-tables [:pet/index :person/number])
        ["pet" "person"])))

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


(deftest joins->self-join-source-column-aliases-test
  (is (= (sut/joins->self-join-source-column-aliases
           {:human/pet    [:human/pet-index :pet/index]
            :human/follow [:human/number :follow/human-1 :follow/human-2 :human/number]})
        {:human/follow :human-1/number})))

(deftest ->join-statements-tests
  (is (= (sut/->join-statements [:pet/index :person-pet/pet-index
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
        {:child-join-keys #{:pet/owner}
         :columns-to-query #{:pet/yob :person/number}})))

(deftest get-child-env-test
  (let [sql-schema #::sut {:column-keywords
                           #{:pet/yob}
                           :required-columns
                           {:pet/age #{:pet/yob}}
                           :source-columns
                           {:person/pets :person/number
                            :pet/owner   :person/number
                            :pet/species :species/id}}]
    (is (= (sut/get-child-env
             {:ast {:children [{:dispatch-key :person/name}
                               {:dispatch-key :person/pets ;; <- it's here
                                :children     [{:dispatch-key :pet/name}
                                               {:dispatch-key :pet/owner
                                                :children     [{:dispatch-key :person/name}]}
                                               {:dispatch-key :pet/species
                                                :children     [{:dispatch-key :species/name}]}]}]}
              ::sut/sql-schema
              sql-schema}
             :person/pets)
          {:ast {:dispatch-key :person/pets,
                 :children     [{:dispatch-key :pet/name}
                                {:dispatch-key :pet/owner,
                                 :children     [{:dispatch-key :person/name}]}
                                {:dispatch-key :pet/species,
                                 :children     [{:dispatch-key :species/name}]}]},
           ::sut/sql-schema
           sql-schema}))))

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
                         :person-pet/person-number :person/number]
            :farmer/cow [:farmer/cow-index :cow/index]})
        {:person/pet "person",
         :pet/owner  "pet",
         :farmer/cow "farmer"})))

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
