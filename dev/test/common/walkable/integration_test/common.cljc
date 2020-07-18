(ns walkable.integration-test.common
  (:require [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [plumbing.core :refer [fnk defnk sum]]))

(def farmer-house-floor-plan
  {:true-columns     [:house/color
                      :farmer/number
                      :farmer/name]
   :roots            {:farmers/farmers "farmer"}
   :extra-conditions {}
   :joins            {:farmer/house [:farmer/house-index :house/index]}
   :reversed-joins   {:house/owner :farmer/house}
   :cardinality      {:house/owner  :one
                      :farmer/house :one}})

;; core-configs
(def farmer-house-config
  {:inputs-outputs
   (let [farmer-out [:farmer/number
                     :farmer/name
                     :farmer/house-index
                     :farmer/house
                     :farmer/house-count]
         house-out  [:house/index
                     :house/color
                     :house/owner]]
     [{::pc/output [{:farmers/farmers farmer-out}]}

      {::pc/input  #{:farmer/number}
       ::pc/output farmer-out}

      {::pc/output [{:house/owner farmer-out}]}

      {::pc/output [{:houses/houses house-out}]}

      {::pc/output [{:farmer/house house-out}]}

      {::pc/input  #{:house/index}
       ::pc/output house-out}])})

(def kid-toy-config
  {:inputs-outputs
   (let [kid-out [:kid/number
                  :kid/name
                  :kid/toy]
         toy-out [:toy/index
                  :toy/color
                  :toy/owner]]
     [{::pc/output [{:kids/kids kid-out}]}

      {::pc/output [{:toy/owner kid-out}]}

      {::pc/output [{:toys/toys toy-out}]}

      {::pc/output [{:kid/toy toy-out}]}

      {::pc/input  #{:kid/number}
       ::pc/output kid-out}

      {::pc/input  #{:toy/index}
       ::pc/output toy-out}])})

(def human-follow-config
  {:inputs-outputs
   (let [human-out [:human/number
                    :human/name
                    :human/yob
                    :human/follow
                    :human/follow-stats
                    :human/age
                    :human/stats]]
     [{::pc/output [{:humans/humans human-out}]}

      {::pc/input  #{:human/number}
       ::pc/output human-out}

      {::pc/output {:human/follow human-out}}])})

(def person-pet-config
  {:inputs-outputs
   (let [person-out [:person/number
                     :person/name
                     :person/age
                     :person/yob
                     :person/hidden
                     :person/pet
                     :person/pet-count]
         pet-out    [:pet/index
                     :pet/yob
                     :pet/color
                     :pet/owner]]
     [{::pc/output [{:people/people person-out}]}

      {::pc/output [{:pets/by-color [:pet/color :color/pet-count]}]}

      {::pc/output [:people/count]}

      {::pc/input  #{:person/number}
       ::pc/output person-out}

      {::pc/output [{:pet/owner (into person-out [:person-pet/pet-index :person-pet/person-number :person-pet/adoption-year])}]}

      {::pc/output [{:pets/pets pet-out}]}

      {::pc/output [{:person/pet (into pet-out [:person-pet/pet-index :person-pet/person-number :person-pet/adoption-year])}]}

      {::pc/input  #{:pet/index}
       ::pc/output pet-out}])})

;; ends core-configs

(def kid-toy-floor-plan
  {:true-columns     [:kid/name :toy/index :toy/color]
   :roots            {:kids/kids "kid"}
   :extra-conditions {}
   :joins            {:toy/owner [:toy/owner-number :kid/number]}
   :reversed-joins   {:kid/toy :toy/owner}
   :cardinality      {:kid/toy   :one
                      :toy/owner :one}})

(def human-follow-floor-plan
  {:true-columns     [:human/number :human/name :human/yob]
   :required-columns {}
   :roots            {:humans/humans "human"}
   :extra-conditions {}
   :joins            {:human/follow
                      [:human/number :follow/human-1 :follow/human-2 :human/number]}
   :reversed-joins   {}
   :cardinality      {:human/follow-stats :one
                      :human/follow       :many}})

(def person-pet-floor-plan
  {:true-columns     [:person/name
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
   :roots            {:pets/by-color "pet"
                      [:people/people :people/count] "person"}
   :extra-conditions {[:person/number :people/people :people/count]
                      [:or [:= :person/hidden true]
                       [:= :person/hidden false]]}
   :joins            {[:person/pet :person/pet-count]
                      [:person/number :person-pet/person-number
                       :person-pet/pet-index :pet/index]}
   :reversed-joins   {:pet/owner :person/pet}
   :pseudo-columns   {:person/age      [:- 2018 :person/yob]
                      :color/pet-count [:count :pet/index]}
   :aggregators      {[:people/count :person/pet-count]
                      [:count-*]}
   :grouping         {:pets/by-color
                      {:group-by [:pet/color]
                       :having   [:< 1 [:color/pet-count]]}}
   :cardinality      {:person/pet :many}})

(def common-scenarios
  {:farmer-house
   {:core-floor-plan farmer-house-floor-plan
    :core-config     farmer-house-config
    :test-suite
    [{:message "filters should work"
      :query
      `[{(:farmers/farmers {:filters {:farmer/house [{:house/owner [:= :farmer/name "mary"]}]}})
         [:farmer/number :farmer/name
          {:farmer/house [:house/index :house/color]}]}]
      :expected
      {:farmers/farmers [#:farmer{:number 2, :name "mary", :house #:house {:index "20", :color "brown"}}]}}
     {:message "no pagination"
      :query
      `[{:farmers/farmers
         [:farmer/number :farmer/name
          {:farmer/house [:house/index :house/color]}]}]
      :expected
      #:farmers {:farmers [#:farmer{:number 1, :name "jon", :house #:house {:index "10", :color "black"}}
                           #:farmer{:number 2, :name "mary", :house #:house {:index "20", :color "brown"}}
                           #:farmer{:number 3, :name "homeless", :house #:house {}}]}}]}
   :farmer-house-paginated
   {:core-config     farmer-house-config
    :core-floor-plan (assoc farmer-house-floor-plan
                       :pagination-fallbacks
                       {:farmers/farmers
                        {:order-by {:default  [:farmer/name :desc]
                                    :validate #{:farmer/name :farmer/number}}}})
    :test-suite
    [{:message "pagination fallbacks"
      :query
      `[{:farmers/farmers
         [:farmer/number :farmer/name
          {:farmer/house [:house/index :house/color]}]}]
      :expected
      #:farmers{:farmers [#:farmer{:number 2, :name "mary", :house #:house {:index "20", :color "brown"}}
                          #:farmer{:number 1, :name "jon", :house #:house {:index "10", :color "black"}}
                          #:farmer{:number 3, :name "homeless", :house #:house {}}]}}
     {:message "supplied pagination"
      :query
      `[{(:farmers/farmers {:limit 1})
         [:farmer/number :farmer/name
          {:farmer/house [:house/index :house/color]}]}]
      :expected
      #:farmers{:farmers [#:farmer{:number 2, :name "mary", :house #:house {:index "20", :color "brown"}}]}}
     {:message "without order-by column in query"
      :query
      `[{(:farmers/farmers {:limit 1})
         [:farmer/number
          {:farmer/house [:house/index :house/color]}]}]
      :expected
      #:farmers{:farmers [#:farmer{:number 2, :house #:house {:index "20", :color "brown"}}]}}]}
   :kid-toy
   {:core-floor-plan kid-toy-floor-plan
    :core-config     kid-toy-config
    :test-suite
    [{:message "idents should work"
      :query
      '[{[:kid/number 1] [:kid/number :kid/name
                          {:kid/toy [:toy/index :toy/color]}]}]
      :expected
      {[:kid/number 1] #:kid {:number 1, :name "jon", :toy #:toy {:index 10, :color "yellow"}}}}]}

   :human-follow
   {:core-floor-plan human-follow-floor-plan
    :core-config     human-follow-config
    :test-suite
    [{:message "self-join should work"
      :query
      `[{(:humans/humans {:order-by :human/number})
         [:human/number :human/name
          {(:human/follow {:order-by :human/number})
           [:human/number
            :human/name
            :human/yob]}]}]
      :expected
      {:humans/humans [#:human{:number 1, :name "jon",
                               :follow [#:human{:number 2, :name "mary", :yob 1992}
                                        #:human{:number 3, :name "peter", :yob 1989}
                                        #:human{:number 4, :name "sandra", :yob 1970}]}
                       #:human{:number 2, :name "mary",
                               :follow [#:human{:number 1, :name "jon", :yob 1980}
                                        #:human{:number 3, :name "peter", :yob 1989}]}
                       #:human{:number 3, :name "peter", :follow []}
                       #:human{:number 4, :name "sandra", :follow []}]}}]}

   :human-follow-variable-getters
   {:core-config     human-follow-config
    :core-floor-plan (merge human-follow-floor-plan
                       {:pseudo-columns
                        {:human/age   [:- 'current-year :human/yob]
                         :human/stats [:str 'stats-header
                                       ", m: " 'm
                                       ", v: " 'v]}}
                       {:variable-getters
                        [{:key 'current-year
                          :fn  (fn [env] (:current-year env))}]
                        :variable-getter-graphs
                        [{:graph
                          {:xs           (fnk [env] (get-in env [:xs]))
                           :n            (fnk [xs] (count xs))
                           :m            (fnk [xs n] (/ (sum identity xs) n))
                           :m2           (fnk [xs n] (/ (sum #(* % %) xs) n))
                           :v            (fnk [m m2] (str (- m2 (* m m))))
                           :stats-header (fnk [xs] (str "stats for xs =" (pr-str xs)))}}]})
    :test-suite
    [{:message "variable-getters should work"
      :env     {:current-year 2019}
      :query
      `[{(:humans/humans {:order-by :human/number})
         [:human/number :human/name :human/age
          {(:human/follow {:order-by :human/number})
           [:human/number
            :human/name
            :human/age]}]}]
      :expected
      {:humans/humans [#:human{:number 1, :name "jon", :age 39,
                               :follow [#:human{:number 2, :name "mary", :age 27}
                                        #:human{:number 3, :name "peter", :age 30}
                                        #:human{:number 4, :name "sandra", :age 49}]}
                       #:human{:number 2, :name "mary", :age 27,
                               :follow [#:human{:number 1, :name "jon", :age 39}
                                        #:human{:number 3, :name "peter", :age 30}]}
                       #:human{:number 3, :name "peter", :age 30, :follow []}
                       #:human{:number 4, :name "sandra", :age 49, :follow []}]}}
     {:message "variable-getter-graphs should work"
      ;; choose this sequence so stats values are integers
      ;; therefore the output string is the same in all sql dbs
      :env     {:xs [2 4 6 8]}
      :query
      `[{(:humans/humans {:order-by :human/number})
         [:human/number :human/name :human/stats]}]
      :expected
      #:humans{:humans [#:human{:number 1, :name "jon",
                                :stats  "stats for xs =[2 4 6 8], m: 5, v: 5"}
                        #:human{:number 2, :name "mary",
                                :stats  "stats for xs =[2 4 6 8], m: 5, v: 5"}
                        #:human{:number 3, :name "peter",
                                :stats  "stats for xs =[2 4 6 8], m: 5, v: 5"}
                        #:human{:number 4, :name "sandra",
                                :stats  "stats for xs =[2 4 6 8], m: 5, v: 5"}]}}]}
   :person-pet
   {:core-floor-plan person-pet-floor-plan
    :core-config     person-pet-config
    :test-suite
    [{:message "join-table should work"
      :query
      `[{[:person/number 1]
         [:person/number
          :person/name
          :person/yob
          {:person/pet [:pet/index
                        :pet/yob
                        :pet/color
                        :person-pet/adoption-year
                        {:pet/owner [:person/name]}]}]}]
      :expected
      {[:person/number 1]
       #:person {:number 1,
                 :name   "jon",
                 :yob    1980,
                 :pet
                 [{:pet/index                10,
                   :pet/yob                  2015,
                   :pet/color                "yellow",
                   :person-pet/adoption-year 2015,
                   :pet/owner                [#:person{:name "jon"}]}
                  {:pet/index                11,
                   :pet/yob                  2012,
                   :pet/color                "green",
                   :person-pet/adoption-year 2013,
                   :pet/owner                [#:person{:name "jon"}]}
                  {:pet/index                12,
                   :pet/yob                  2017,
                   :pet/color                "yellow",
                   :person-pet/adoption-year 2018,
                   :pet/owner                [#:person{:name "jon"}]}
                  {:pet/index                13,
                   :pet/yob                  2016,
                   :pet/color                "green",
                   :person-pet/adoption-year 2016,
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
      `[{(:people/people {:filters  [:and {:person/pet [:or [:= :pet/color "white"]
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
      #:people{:people
               [#:person{:number    1,
                         :name      "jon",
                         :pet-count 4,
                         :pet
                         [{:pet/index                10,
                           :pet/yob                  2015,
                           :person-pet/adoption-year 2015,
                           :pet/color                "yellow"}
                          {:pet/index                11,
                           :pet/yob                  2012,
                           :person-pet/adoption-year 2013,
                           :pet/color                "green"}
                          {:pet/index                12,
                           :pet/yob                  2017,
                           :person-pet/adoption-year 2018,
                           :pet/color                "yellow"}
                          {:pet/index                13,
                           :pet/yob                  2016,
                           :person-pet/adoption-year 2016,
                           :pet/color                "green"}]}]}}

     {:message "placeholders should work"
      :env     {::p/placeholder-prefixes #{"ph" "placeholder"}}
      :query
      `[{:people/people
         [{:placeholder/info [:person/yob :person/name]}
          {:person/pet [:pet/index
                        :pet/yob
                        :pet/color]}
          {:ph/deep [{:ph/nested [{:placeholder/play [{:person/pet [:pet/index
                                                                    :pet/yob
                                                                    :pet/color]}]}]}]}]}]
      :expected
      #:people{:people
               [{:placeholder/info #:person {:yob 1980, :name "jon"},
                 :person/pet
                 [#:pet{:index 10, :yob 2015, :color "yellow"}
                  #:pet{:index 11, :yob 2012, :color "green"}
                  #:pet{:index 12, :yob 2017, :color "yellow"}
                  #:pet{:index 13, :yob 2016, :color "green"}],
                 :ph/deep
                 #:ph              {:nested
                                    #:placeholder {:play
                                                   #:person {:pet
                                                             [#:pet{:index 10,
                                                                    :yob   2015,
                                                                    :color "yellow"}
                                                              #:pet{:index 11,
                                                                    :yob   2012,
                                                                    :color "green"}
                                                              #:pet{:index 12,
                                                                    :yob   2017,
                                                                    :color "yellow"}
                                                              #:pet{:index 13,
                                                                    :yob   2016,
                                                                    :color "green"}]}}}}
                {:placeholder/info #:person {:yob 1992, :name "mary"},
                 :person/pet
                 [#:pet{:index 20, :yob 2014, :color "orange"}
                  #:pet{:index 21, :yob 2015, :color "green"}
                  #:pet{:index 22, :yob 2016, :color "green"}],
                 :ph/deep
                 #:ph              {:nested
                                    #:placeholder {:play
                                                   #:person {:pet
                                                             [#:pet{:index 20,
                                                                    :yob   2014,
                                                                    :color "orange"}
                                                              #:pet{:index 21,
                                                                    :yob   2015,
                                                                    :color "green"}
                                                              #:pet{:index 22,
                                                                    :yob   2016,
                                                                    :color "green"}]}}}}]}}

     {:message "grouping should work"
      :query
      `[{(:pets/by-color {:order-by [:pet/color :desc]})
         [:pet/color]}]
      :expected
      #:pets   {:by-color
                [{:pet/color "yellow"}
                 {:pet/color "green"}]}}

     {:message "grouping with count should work"
      :query
      `[{(:pets/by-color {:order-by [:pet/color :asc]})
         [:color/pet-count :pet/color]}]
      :expected
      #:pets{:by-color
             [{:color/pet-count 4, :pet/color "green"}
              {:color/pet-count 2, :pet/color "yellow"}]}}

     {:message "pseudo-columns should work"
      :query
      `[{:people/people [:person/number
                         :person/name
                         :person/yob
                         :person/age]}]
      :expected
      #:people {:people [#:person{:number 1, :name "jon", :yob 1980, :age 38}
                         #:person{:number 2, :name "mary", :yob 1992, :age 26}]}}]}})
