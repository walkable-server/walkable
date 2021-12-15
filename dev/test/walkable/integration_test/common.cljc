(ns walkable.integration-test.common
  (:require [com.wsscode.pathom.core :as p]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [plumbing.core :refer [fnk sum]]))

(def farmer-house-registry
  [{:key :farmers/farmers
    :type :root
    :table "farmer"
    :output [:farmer/number :farmer/name :farmer/house]}
   {:key :farmer/number
    :type :true-column
    :primary-key true
    :output [:farmer/name :farmer/house-plus :farmer/house-count :farmer/house]}
   {:key :farmer/house
    :type :join
    :join-path [{:farmer/house-index :house/index}]
    :output [:house/color]
    :cardinality :one}
   {:key :house/owner
    :type :join
    :join-path [{:house/index :farmer/house-index}]
    :output [:farmer/number]
    :cardinality :one}])

(def kid-toy-registry
  [{:key :kids/kids
    :type :root
    :table "kid"
    :output [:kid/name]}
   {:key :toy/owner
    :type :join
    :join-path [{:toy/owner-number :kid/number}]
    :output [:kid/name :kid/number]
    :cardinality :one}
   {:key :kid/toy
    :type :join
    :join-path [{:kid/number :toy/owner-number}]
    :output [:toy/color :toy/index]
    :cardinality :one}
   {:key :kid/number
    :type :true-column
    :primary-key true
    :output [:kid/name]}])

(def human-follow-registry
  [{:key :humans/humans
    :type :root
    :table "human"
    :output [:human/number :human/name :human/yob :human/age]}
   {:key :human/follow
    :type :join
    :join-path [{:human/number :follow/human-1}
                {:follow/human-2 :human/number}]
    :output [:human/number :human/name :human/yob :human/age]}])

(def person-pet-registry
  [{:key :people/people
    :type :root
    :table "person"
    :output [:person/number]
    :filter
    [:or [:= :person/hidden true]
     [:= :person/hidden false]]}
   {:key :person/age
    :type :pseudo-column
    :formula [:- 2018 :person/yob]}
   {:key :person/number
    :type :true-column
    :primary-key true
    :output [:person/name :person/yob :person/age]
    :filter
    [:or [:= :person/hidden true]
     [:= :person/hidden false]]}
   {:key :people/count
    :type :root
    :aggregate true
    :table "person"
    :formula [:count-*]
    :filter
    [:or [:= :person/hidden true]
     [:= :person/hidden false]]}
   {:key :person/pet
    :type :join
    :join-path
    [{:person/number :person-pet/person-number}
     {:person-pet/pet-index :pet/index}]
    :output [:pet/index
             :person-pet/adoption-year
             :pet/name
             :pet/yob
             :pet/color]}
   {:key :pet/owner
    :type :join
    :join-path
    [{:pet/index :person-pet/pet-index}
     {:person-pet/person-number :person/number}]
    :output [:person/number]}
   {:key :person/pet-count
    :type :join
    :join-path
    [{:person/number :person-pet/person-number}
     {:person-pet/pet-index :pet/index}]
    :aggregate true
    :formula [:count-*]}
   {:key :pets/by-color
    :type :root
    :table "pet"
    :output [:pet/color :color/pet-count]
    :group-by [:pet/color]
    :having [:< 1 :color/pet-count]}
   {:key :color/pet-count
    :type :pseudo-column
    :formula [:count :pet/index]}])

(def article-revision-registry
  [{:key :articles/list
    :type :root
    :table "article"
    :output [:article/id :article/title :article/current-revision]}
   {:key :article/revision
    :type :join
    :cardinality :one
    :output [:revision/content]
    :join-path [{:article/id :revision/id
                 :article/current-revision :revision/revision}]}])

(def common-scenarios
  {:farmer-house
   {:registry farmer-house-registry
    :test-suite
    [{:message "filters should work"
      :query
      `[{(:farmers/farmers {:filter {:farmer/house {:house/owner [:= :farmer/name "mary"]}}})
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
   {:registry (floor-plan/conditionally-update
                farmer-house-registry
                #(=  :farmers/farmers (:key %))
                #(merge % {:default-order-by [:farmer/name :desc]
                           :validate-order-by #{:farmer/name :farmer/number}}))
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
   {:registry kid-toy-registry
    :test-suite
    [{:message "idents should work"
      :query
      '[{[:kid/number 1] [:kid/number :kid/name
                          {:kid/toy [:toy/index :toy/color]}]}]
      :expected
      {[:kid/number 1] #:kid {:number 1, :name "jon", :toy #:toy {:index 10, :color "yellow"}}}}]}

   :human-follow
   {:registry human-follow-registry
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
   {:registry (concat human-follow-registry
                [{:key :human/age
                  :type :pseudo-column
                  :formula [:- 'current-year :human/yob]}
                 {:key :human/stats
                  :type :pseudo-column
                  :formula [:str 'stats-header
                            ", m: " 'm
                            ", v: " 'v]}
                 {:key 'current-year
                  :type :variable
                  :compute (fn [env] (:current-year env))}
                 ;; TODO: convention for variable-graph's :key
                 {:key [:graph :hello]
                  :type :variable-graph
                  :graph
                  {:xs (fnk [env] (get-in env [:xs]))
                   :n (fnk [xs] (count xs))
                   :m (fnk [xs n] (/ (sum identity xs) n))
                   :m2 (fnk [xs n] (/ (sum #(* % %) xs) n))
                   :v (fnk [m m2] (str (- m2 (* m m))))
                   :stats-header (fnk [xs] (str "stats for xs =" (pr-str xs)))}}]) 
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
     #_{:message "variable-getter-graphs should work"
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
   {:registry person-pet-registry
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
      `[(:people/count {:filter [:and {:person/pet [:or [:= :pet/color "white"]
                                                    [:= :pet/color "yellow"]]}
                                 [:< :person/number 10]]})]
      :expected
      #:people {:count 1}}

     {:message "filters should work"
      :query
      `[{(:people/people {:filter [:and {:person/pet [:or [:= :pet/color "white"]
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
                         #:person{:number 2, :name "mary", :yob 1992, :age 26}]}}]}

   :article-revision
   {:registry article-revision-registry
    :test-suite
    [{:message "join with multiple column pairs should work"
      :query `[{:articles/list [:article/id :article/title
                                {:article/revision [:revision/content]}]}]
      :expected
      #:articles{:list
                 [#:article{:id 1,
                            :title "introduction",
                            :revision
                            #:revision{:content "this is sparta"}}
                  #:article{:id 2,
                            :title "hello world",
                            :revision
                            #:revision{:content "welcome to my site"}}]}}]}})
