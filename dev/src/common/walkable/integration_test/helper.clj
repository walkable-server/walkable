(ns walkable.integration-test.helper
  (:require [clojure.java.jdbc :as jdbc]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [walkable.core :as walkable]
            [walkable.sql-query-builder.emitter :as emitter]
            [clojure.test :refer [testing is]]))

(def db-specific-emitter
  {:mysql    emitter/mysql-emitter
   :sqlite   emitter/sqlite-emitter
   :postgres emitter/postgres-emitter})

(defn core-config
  [{:keys [db-type core-floor-plan cte?]}]
  (let [resolver-sym `my-resolver]
    {:resolver-sym resolver-sym
     :index-oir
     (merge
       {:farmers/farmers {#{} #{resolver-sym}}
        :houses/houses  {#{} #{resolver-sym}}}

       #:farmer{:number      {#{:farmers/farmers} #{resolver-sym}}
                :name        {#{:farmers/farmers} #{resolver-sym}}
                :house-index {#{:farmers/farmers} #{resolver-sym}}
                :house       {#{:farmer/number}      #{resolver-sym}
                              #{:farmer/house-index} #{resolver-sym}}}
       #:house{:index        {#{:farmer/house} #{resolver-sym}
                              #{:houses/houses}   #{resolver-sym}}
               :color        {#{:house/index} #{resolver-sym}
                              #{:houses/houses}  #{resolver-sym}}
               :owner-number {#{:house/index} #{resolver-sym}
                              #{:houses/houses}  #{resolver-sym}}
               :owner        {#{:house/index} #{resolver-sym}
                              #{:houses/houses}  #{resolver-sym}}})

     :index-io
     {#{}
      {:farmers/farmers #:farmer{:number {} :name {} :house-index {} :house #:house{:index {}}}
       :houses/houses #:house{:index {} :color {} :owner #:farmer {:number {}}}}

      #{:farmer/number}
      #:farmer{:name {} :house-index {}}

      #{:farmer/house-index}
      #:house{:index {} :color {}
              :owner #:farmer{:number {}}}

      #{:house/index}
      #:house{:color {}
              :owner #:farmer{:number {}}}}

     :index-idents
     #{:farmer/number :house/index}

     :floor-plan
     (merge core-floor-plan
       {:emitter          (db-specific-emitter db-type)
        :use-cte          (when cte? {:default true})
        ;; columns already declared in :joins are not needed
        ;; here
        :true-columns     [:house/color
                           :farmer/number
                           :farmer/name]
        :idents           {:farmer/id       :farmer/number
                           :farmers/farmers "farmer"
                           :houses/houses   "house"}
        :extra-conditions {}
        :joins            {:farmer/house [:farmer/house-index :house/index]}
        :reversed-joins   {:house/owner :farmer/house}
        :cardinality      {:farmer/id    :one
                           :house/owner  :one
                           :farmer/house :one}})}))

(defn walkable-parser
  [{:keys [db] :as config}]
  (p/parser
    {::p/env     {::p/reader               [p/map-reader
                                            pc/reader2
                                            pc/open-ident-reader
                                            p/env-placeholder-reader]
                  ::p/placeholder-prefixes #{">"}}
     ::p/plugins [(pc/connect-plugin {::pc/register []})
                  (walkable/connect-plugin (assoc (core-config config)
                                             :db db
                                             :query jdbc/query))
                  p/error-handler-plugin
                  p/trace-plugin]}))

(defn run-scenario-tests
  [db db-type scenarios]
  (into []
    (for [[scenario {:keys [core-floor-plan test-suite]}] scenarios
          {:keys [message env query expected]}            test-suite]
      (testing (str "In scenario " scenario " for " db-type ", testing " message)
        (is (= expected
              (let [parser (walkable-parser {:db db
                                             :core-floor-plan core-floor-plan
                                             :db-type db-type})]
                (parser (assoc env ::p/placeholder-prefixes #{"ph"}) query)))
          "without CTEs in joins")
        (is (= expected
              (let [parser (walkable-parser {:db db
                                             :core-floor-plan core-floor-plan
                                             :db-type db-type
                                             :cte? true})]
                (parser (assoc env ::p/placeholder-prefixes #{"ph"}) query)))
          "with CTEs in joins")))))
