(ns walkable.integration-test.common)

(def farmer-cow-schema
  {:columns          [:cow/color
                      :farmer/number
                      :farmer/name]
   :idents           {:farmer/by-id :farmer/number
                      :farmers/all  "farmer"}
   :extra-conditions {}
   :joins            {:farmer/cow [:farmer/cow-index :cow/index]}
   :reversed-joins   {:cow/owner :farmer/cow}
   :cardinality      {:farmer/by-id :one
                      :cow/owner    :one
                      :farmer/cow   :one}})

(def kid-toy-schema
  {:columns          [:kid/name :toy/index :toy/color]
   :idents           {:kid/by-id :kid/number
                      :kids/all  "kid"}
   :extra-conditions {}
   :joins            {:toy/owner [:toy/owner-number :kid/number]}
   :reversed-joins   {:kid/toy :toy/owner}
   :cardinality      {:kid/by-id :one
                      :kid/toy   :one
                      :toy/owner :one}})

(def person-pet-schema
  {:columns          [:person/name
                      :person/yob
                      :person/hidden
                      :person-pet/adoption-year
                      :pet/name
                      :pet/yob
                      :pet/color]
   ;; Extra columns required when an attribute is being asked for.
   ;; Can be input to derive attributes, or parameters to other
   ;; attribute resolvers that will run SQL queries themselves
   :required-columns {:pet/age    #{:pet/yob}
                      :person/age #{:person/yob}}
   :idents           {:person/by-id               :person/number
                      [:people/all :people/count] "person"}
   :extra-conditions {[:person/by-id :people/all :people/count]
                      [:or [:= :person/hidden true]
                       [:= :person/hidden false]]}
   :joins            {[:person/pet :person/pet-count]
                      [:person/number :person-pet/person-number
                       :person-pet/pet-index :pet/index]}
   :reversed-joins   {:pet/owner :person/pet}
   :pseudo-columns   {:person/age [:- 2018 :person/yob]}
   :aggregators      {[:people/count :person/pet-count]
                      [:count-*]}
   :cardinality      {:person/by-id :one
                      :person/pet   :many}})

(def common-scenarios
  {:farmer-cow
   {:core-schema farmer-cow-schema
    :test-suite
    [{:message "filters should work"
      :query
      `[{(:farmers/all {:filters {:farmer/cow [{:cow/owner [:= :farmer/name "mary"]}]}})
         [:farmer/number :farmer/name
          {:farmer/cow [:cow/index :cow/color]}]}]
      :expected
      {:farmers/all [#:farmer{:number 2, :name "mary", :cow #:cow {:index 20, :color "brown"}}]}}]}

   :kid-toy
   {:core-schema kid-toy-schema
    :test-suite
    [{:message "idents should work"
      :query
      '[{[:kid/by-id 1] [:kid/number :kid/name
                         {:kid/toy [:toy/index :toy/color]}]}]
      :expected
      {[:kid/by-id 1] #:kid {:number 1, :name "jon", :toy #:toy {:index 10, :color "yellow"}}}}]}

   :person-pet
   {:core-schema person-pet-schema
    :test-suite
    [{:message "join-table should work"
      :query
      `[{[:person/by-id 1]
         [:person/number
          :person/name
          :person/yob
          {:person/pet [:pet/index
                        :pet/yob
                        :pet/color
                        :person-pet/adoption-year
                        {:pet/owner [:person/name]}]}]}]
      :expected
      {[:person/by-id 1]
       #:person {:number 1,
                 :name   "jon",
                 :yob    1980,
                 :pet    [{:pet/index                10,
                           :pet/yob                  2015,
                           :pet/color                "yellow",
                           :person-pet/adoption-year 2015,
                           :pet/owner                [#:person{:name "jon"}]}]}}}

     {:message "aggregate should work"
      :query
      `[(:people/count {:filters [:and {:person/pet [:or [:= :pet/color "white"]
                                                     [:= :pet/color "yellow"]]}
                                  [:< :person/number 10]]})]
      :expected
      #:people {:count 1}}

     {:message "filters should work"
      :query
      `[{(:people/all {:filters  [:and {:person/pet [:or [:= :pet/color "white"]
                                                     [:= :pet/color "yellow"]]}
                                  [:< :person/number 10]]
                       :order-by [:person/name]})
         [:person/number :person/name
          :person/pet-count
          {:person/pet [:pet/index
                        :pet/yob
                        ;; columns from join table work, too
                        :person-pet/adoption-year
                        :pet/color]}]}]
      :expected
      #:people {:all [#:person{:number    1,
                               :name      "jon",
                               :pet-count 1,
                               :pet       [{:pet/index                10,
                                            :pet/yob                  2015,
                                            :person-pet/adoption-year 2015,
                                            :pet/color                "yellow"}]}]}}

     {:message "pseudo-columns should work"
      :query
      `[{:people/all [:person/number
                      :person/name
                      :person/yob
                      :person/age]}]
      :expected
      #:people{:all [#:person{:number 1, :name "jon", :yob 1980, :age 38}
                     #:person{:number 2, :name "mary", :yob 1992, :age 26}]}}]}})
