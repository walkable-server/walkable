(ns dev
  (:require [com.wsscode.pathom.core :as p]
            [walkable.sql-query-builder :as sqb]
            ["sqlite3" :as sqlite3]
            [cljs.core.async :as async :refer [put! >! <! promise-chan]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

;; (enable-console-print!)

(def sqlite-db (sqlite3/Database. "walkable_demo.sqlite"))

(def quote-marks sqb/backticks)

(def sqlite-union true)

(def async-parser
  (p/async-parser
    {::p/plugins
     [(p/env-plugin
        {::p/reader
         [sqb/async-pull-entities p/map-reader]})]}))

(defn async-run-print-query
  [db [q & params]]
  (let [c (promise-chan)]
    (.all db q (or (to-array params) #js [])
      (fn callback [e r]
        (let [x (js->clj r :keywordize-keys true)]
          (println "\nsql query: " q)
          (println "sql params: " params)
          (println "sql results:" x)
          (put! c x))))
    c))

;; all queries have been comment out
;; uncomment them one by one to see them in action
#_
(let [eg-1
      '[{[:farmer/by-id 1] [:farmer/number :farmer/name
                            {:farmer/cow [:cow/index :cow/color]}]}]

      parser async-parser]
  (go
    (println "\n\n<-- final result -->\n"
      (<! (parser {::sqb/sql-db
                   sqlite-db
                   ::sqb/run-query
                   async-run-print-query
                   ::sqb/sql-schema
                   (sqb/compile-schema
                     {:quote-marks      quote-marks
                      :sqlite-union     sqlite-union
                      ;; columns already declared in :joins are not needed
                      ;; here
                      :columns          [:cow/color
                                         :farmer/number
                                         :farmer/name]
                      :idents           {:farmer/by-id :farmer/number
                                         :farmers/all  "farmer"}
                      :extra-conditions {}
                      :joins            {:farmer/cow [:farmer/cow-index :cow/index]}
                      :reversed-joins   {:cow/owner :farmer/cow}
                      :cardinality      {:farmer/by-id :one
                                         :cow/owner    :one
                                         :farmer/cow   :one}})}
            eg-1)))))
#_
(let [eg-1
      '[{[:kid/by-id 1] [:kid/number :kid/name
                         {:kid/toy [:toy/index :toy/color]}]}]

      parser
      async-parser]
  (println "\n\n")
  (go
    (println "final result"
      (<!
        (parser {::sqb/sql-db    sqlite-db
                 ::sqb/run-query async-run-print-query

                 ::sqb/sql-schema
                 (sqb/compile-schema
                   {:quote-marks      quote-marks
                    :sqlite-union     sqlite-union
                    :columns          [:kid/name :toy/index :toy/color]
                    :idents           {:kid/by-id :kid/number
                                       :kids/all  "kid"}
                    :extra-conditions {}
                    :joins            {:toy/owner [:toy/owner-number :kid/number]}
                    :reversed-joins   {:kid/toy :toy/owner}
                    :cardinality      {:kid/by-id :one
                                       :kid/toy   :one
                                       :toy/owner :one}})}
          eg-1)))))
#_
(let [eg-1
      '[{(:people/all {:filters  [:< :person/number 10]
                       :limit    1
                       :offset   1
                       :order-by [:person/name]})
         [:person/number :person/name
          {:person/pet [:pet/index
                        :pet/yob
                        ;; columns from join table work, too
                        :person-pet/adoption-year
                        :pet/color]}]}]

      eg-2
      '[{[:person/by-id 1]
         [:person/number
          :person/name
          :person/yob
          {:person/pet [:pet/index
                        :pet/yob
                        :pet/color
                        :person-pet/adoption-year
                        {:pet/owner [:person/name]}]}]}]
      parser
      async-parser]
  (go
    (println "final result: "
      (<!
        (parser {::sqb/sql-db    sqlite-db
                 ::sqb/run-query async-run-print-query
                 ::sqb/sql-schema
                 (sqb/compile-schema
                   ;; which columns are available in SQL table?
                   {:quote-marks      quote-marks
                    :sqlite-union     sqlite-union
                    :columns          [:person/name
                                       :person/yob
                                       :person/hidden
                                       :person-pet/adoption-year
                                       :pet/name
                                       :pet/yob
                                       :pet/color]
                    ;; extra columns required when an attribute is being asked for
                    ;; can be input to derive attributes, or parameters to other attribute resolvers that will run SQL queries themselves
                    :required-columns {:pet/age    #{:pet/yob}
                                       :person/age #{:person/yob}}
                    :idents           {:person/by-id :person/number
                                       :people/all   "person"}
                    :extra-conditions {[:person/by-id :people/all]
                                       [:or [:= :person/hidden true]
                                        [:= :person/hidden false]]}
                    :joins            {:person/pet [:person/number :person-pet/person-number
                                                    :person-pet/pet-index :pet/index]}
                    :reversed-joins   {:pet/owner :person/pet}
                    :cardinality      {:person/by-id :one
                                       :person/pet   :many}})}
          ;; try eg-2, too
          eg-1)))))
#_
(let [eg-1
      '[{:me [:person/number :person/name :person/yob]}]
      parser
      async-parser]
  (go
    (println "final result: "
      (<!
        (parser { ;; extra env data, eg current user id provided by Ring session
                 :current-user 1

                 ::sqb/sql-db    sqlite-db
                 ::sqb/run-query async-run-print-query
                 ::sqb/sql-schema
                 (sqb/compile-schema
                   ;; which columns are available in SQL table?
                   {:quote-marks      quote-marks
                    :sqlite-union     sqlite-union
                    :columns          [:person/number :person/name :person/yob]
                    ;; extra columns required when an attribute is being asked for
                    ;; can be input to derive attributes, or parameters to other attribute resolvers that will run SQL queries themselves
                    :idents           {:me "person"}
                    :extra-conditions {:me
                                       (fn [{:keys [current-user] :as env}]
                                         [:= :person/number current-user])}})}
          eg-1)))))
#_
(let [eg-1
      '[{:people/all
         [{:ph/info [:person/yob :person/name]}
          {:person/pet [:pet/index
                        :pet/yob
                        :pet/color]}
          {:ph/deep [{:ph/nested [{:ph/play [{:person/pet [:pet/index
                                                           :pet/yob
                                                           :pet/color]}]}]}]}]}]
      parser
      (p/async-parser
        {::p/plugins
         [(p/env-plugin
            {::p/reader
             [sqb/async-pull-entities p/env-placeholder-reader p/map-reader]})]})]
  (go
    (println "final result: "
      (<!
        (parser {::p/placeholder-prefixes #{"ph"}
                 ::sqb/sql-db             sqlite-db
                 ::sqb/run-query          async-run-print-query
                 ::sqb/sql-schema
                 (sqb/compile-schema
                   ;; which columns are available in SQL table?
                   {:quote-marks      quote-marks
                    :sqlite-union     sqlite-union
                    :columns          [:person/name
                                       :person/yob
                                       :person/hidden
                                       :pet/name
                                       :pet/yob
                                       :pet/color]
                    ;; extra columns required when an attribute is being asked for
                    ;; can be input to derive attributes, or parameters to other attribute resolvers that will run SQL queries themselves
                    :required-columns {:pet/age    #{:pet/yob}
                                       :person/age #{:person/yob}}
                    :idents           {:person/by-id :person/number
                                       :people/all   "person"}
                    :extra-conditions {}
                    :joins            {:person/pet [:person/number :person-pet/person-number
                                                    :person-pet/pet-index :pet/index]}
                    :reversed-joins   {:pet/owner :person/pet}
                    :cardinality      {:person/by-id :one
                                       :person/pet   :many}})}
          eg-1)))))
#_
(let [eg-1
      '[{:world/all
         [:human/number :human/name
          {:human/follow
           [:human/number
            :human/name
            :human/yob]}]}]
      parser
      (p/async-parser
        {::p/plugins
         [(p/env-plugin
            {::p/reader
             [sqb/async-pull-entities p/map-reader p/error-handler-plugin]})]})]
  (go
    (println "final result:"
      (<!
        (parser {::sqb/sql-db    sqlite-db
                 ::sqb/run-query async-run-print-query
                 ::sqb/sql-schema
                 (sqb/compile-schema
                   {:quote-marks      quote-marks
                    :sqlite-union     sqlite-union
                    :columns          [:human/number :human/name :human/yob]
                    :required-columns {}
                    :idents           {:human/by-id :human/number
                                       :world/all   "human"}
                    :extra-conditions {}
                    :joins            {:human/follow
                                       [:human/number :follow/human-1 :follow/human-2 :human/number]}
                    :reversed-joins   {}
                    :cardinality      {:human/by-id        :one
                                       :human/follow-stats :one
                                       :human/follow       :many}})}
          eg-1)))))
#_
(let [eg-1
      ;; use pseudo-columns in in filters!
      '[{(:world/all {:filters [:= :human/age 38]})
         [:human/number :human/name :human/two
          ;; use pseudo-columns in in filters!
          :human/age
          ;; see :pseudo-columns below
          {:human/follow-stats [:follow/count]}
          {:human/follow [:human/number
                          :human/name
                          :human/yob]}]}]
      parser
      async-parser]
  (go
    (println "final result: "
      (<!
        (parser {::sqb/sql-db    sqlite-db
                 ::sqb/run-query async-run-print-query
                 ::sqb/sql-schema
                 (sqb/compile-schema
                   {:quote-marks      quote-marks
                    :sqlite-union     sqlite-union
                    :columns          [:human/number :human/name :human/yob]
                    :required-columns {}
                    :idents           {:human/by-id :human/number
                                       :world/all   "human"}
                    :extra-conditions {}
                    :joins            { ;; technically :human/follow and :human/follow-stats are the same join
                                       ;; but they have different cardinality
                                       [:human/follow :human/follow-stats]
                                       [:human/number :follow/human-1 :follow/human-2 :human/number]}
                    :reversed-joins   {}
                    :pseudo-columns   { ;; using sub query as a column
                                       :human/age    [:- 2018 :human/yob]
                                       :human/two    2
                                       ;; using aggregate as a column
                                       :follow/count [:count :follow/human-2]
                                       }
                    :cardinality      {:human/by-id        :one
                                       :human/follow-stats :one
                                       :human/follow       :many}})}
          eg-1)))))

(defn main []
  (println "<-- Walkable for nodejs demo -->"))
