(ns dev
  (:refer-clojure :exclude [test])
  (:require [clojure.repl :refer :all]
            [fipp.edn :refer [pprint]]
            [clojure.tools.namespace.repl :refer [refresh]]
            [clojure.java.io :as io]
            [duct.core :as duct]
            [duct.core.repl :as duct-repl]
            [duct.repl.figwheel :refer [cljs-repl]]
            [eftest.runner :as eftest]
            [integrant.core :as ig]
            [duct.database.sql :as sql]
            [duct.logger :as log]
            [duct.database.sql.hikaricp]
            [hikari-cp.core :as hikari-cp]
            [com.wsscode.pathom.core :as p]
            [walkable-demo.handler.example :refer [pathom-parser]]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [integrant.repl :refer [clear halt go init prep reset]]
            [integrant.repl.state :refer [config system]]
            [walkable-demo.handler.example :as example]
            [walkable.sql-query-builder :as sqb]))

;; <<< Beginning of Duct framework helpers
(def wrap-logger #'duct.database.sql.hikaricp/wrap-logger)

;; fix a bug in duct/hikari-cp module
(defmethod ig/init-key :duct.database.sql/hikaricp [_ {:keys [logger] :as options}]
  (sql/->Boundary {:datasource (-> (hikari-cp/make-datasource (dissoc options :logger))
                                 (cond-> logger (wrap-logger logger)))}))

(defn db []
  (-> system (ig/find-derived-1 :duct.database/sql) val :spec))

(defn q [sql]
  (jdbc/query (db) sql))

(defn e [sql]
  (jdbc/execute! (db) sql))

(derive ::devcards :duct/module)

(defmethod ig/init-key ::devcards [_ {build-id :build-id :or {build-id 0}}]
  {:req #{:duct.server/figwheel}
   :fn  #(assoc-in % [:duct.server/figwheel :builds build-id :build-options :devcards] true)})

(duct/load-hierarchy)

(defn read-config []
  (duct/read-config (io/resource "dev.edn")))

(defn test []
  (eftest/run-tests (eftest/find-tests "test")))

(clojure.tools.namespace.repl/set-refresh-dirs "dev/src" "dev/test" "src" "test")

(when (io/resource "local.clj")
  (load "local"))

(integrant.repl/set-prep! (comp duct/prep read-config))

(comment
  (e "CREATE TABLE `foo` (`id` INTEGER)")
  (e "INSERT INTO `foo` (`id`) VALUES (1)")
  (q "SELECT * from foo")
  (e "DROP TABLE `foo`")
  )

;; End of Duct framework helpers >>>

(defn run-print-query
  "jdbc/query wrapped by println"
  [& xs]
  (let [[q & args] (rest xs)]
    (println q)
    (println args))
  (apply jdbc/query xs))

;; use sqb/quotation-marks if you use postgresql
(def quote-marks sqb/backticks)

;; set to false if you use anything else but sqlite
;; eg mysql, postgresql
(def sqlite-union true)

;; Simple join examples

;; I named the primary columns "index" and "number" instead of "id" to
;; ensure arbitrary columns will work.

;; Example: join column living in source table
#_
(let [eg-1
      '[{(:farmers/all {:filters {:farmer/cow [{:cow/owner [:= :farmer/name "mary"]}
                                               [:= :cow/color "brown"]]}})
         [:farmer/number :farmer/name
          {:farmer/cow [:cow/index :cow/color]}]}]

      parser
      example/pathom-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query

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
    eg-1))

;; Example: join column living in target table
#_
(let [eg-1
      '[{[:kid/by-id 1] [:kid/number :kid/name
                         {:kid/toy [:toy/index :toy/color]}]}]

      parser
      example/pathom-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query

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
    eg-1))

;; Example demonstrates:
;; - filters & pagination (offset, limit, order-by) in query
;; - join involving a join table
;; - extra-conditions
;; - derive-attribute plugin
#_
(let [eg-1
      '[{(:people/all {:filters  {:person/pet [:or {:pet/color [:= "white"]}
                                               {:pet/color [:= "yellow"]}]
                                  :person/number [:< 10]}
                       ;; :limit    1
                       ;; :offset   0
                       :order-by [:person/name]})
         [:person/number :person/name
          {:person/pet [:pet/index
                        :pet/age
                        ;; columns from join table work, too
                        :person-pet/adoption-year
                        :pet/color]}]}]

      eg-2
      '[{[:person/by-id 1]
         [:person/number
          :person/name
          :person/age
          {:person/pet [:pet/index
                        :pet/age
                        :pet/color
                        :person-pet/adoption-year
                        {:pet/owner [:person/name]}]}]}]
      parser
      example/pathom-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query
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
                                 [:or {:person/hidden [:= true]}
                                  {:person/hidden [:= false]}]}
              :joins            {:person/pet [:person/number :person-pet/person-number
                                              :person-pet/pet-index :pet/index]}
              :reversed-joins   {:pet/owner :person/pet}
              :cardinality      {:person/by-id :one
                                 :person/pet   :many}})}
    ;; try eg-2, too
    eg-1))

;; advanced filters example
;; lambda form in :extra-conditions
#_
(let [eg-1
      '[{:me [:person/number :person/name :person/yob]}]
      parser
      example/pathom-parser]
  (parser { ;; extra env data, eg current user id provided by Ring session
           :current-user 1

           ::sqb/sql-db    (db)
           ::sqb/run-query run-print-query
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
                                   {:person/number [:= current-user]})}})}
    eg-1))

;; Placeholder example
#_
(let [eg-1
      '[{:people/all
         [{:ph/info [:person/age :person/name]}
          {:person/pet [:pet/index
                        :pet/age
                        :pet/color]}
          {:ph/deep [{:ph/nested [{:ph/play [{:person/pet [:pet/index
                                                           :pet/age
                                                           :pet/color]}]}]}]}]}]
      parser
      example/pathom-parser]
  (parser {::p/placeholder-prefixes #{"ph"}
           ::sqb/sql-db             (db)
           ::sqb/run-query          run-print-query
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
    eg-1))

;; Self-join example
#_
(let [eg-1
      '[{:world/all
         [:human/number :human/name
          {:human/follow [:human/number
                          :human/name
                          :human/yob]}]}]
      parser
      example/pathom-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query
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
    eg-1))

;; :pseudo-columns example
;; experimental - subject to change
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
      example/pathom-parser]
  (parser {::sqb/sql-db    (db)
           ::sqb/run-query run-print-query
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
                                 :human/age ["(? - ?)" 2018 :human/yob]
                                 :human/two    "(SELECT 2)"
                                 ;; using aggregate as a column
                                 :follow/count ["COUNT(?)" :follow/human-2]
                                 }
              :cardinality      {:human/by-id        :one
                                 :human/follow-stats :one
                                 :human/follow       :many}})}
    eg-1))
#_
(q
  "SELECT `person`.`name` AS `person/name`, `person`.`yob` AS `person/yob`
FROM `person`
WHERE
(`person`.`number` IN (SELECT `person_pet`.`person_number`
                      FROM `person_pet` JOIN `pet` ON `person_pet`.`pet_index` = `pet`.`index`
                      WHERE `pet`.`yob` > 1999))
AND
(`person`.`number` > 0)")
(q "SELECT CASE 2 WHEN 2 THEN 2 ELSE 3 END")
