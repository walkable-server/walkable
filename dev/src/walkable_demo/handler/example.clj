(ns walkable-demo.handler.example
  (:require [ataraxy.core :as ataraxy]
            [ataraxy.response :as response]
            [clojure.java.jdbc :as jdbc]
            [walkable.sql-query-builder :as sqb]
            [walkable.sql-query-builder.filters :as sqbf]
            [hikari-cp.core :refer [close-datasource]]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [duct.logger :refer [log]]
            [com.wsscode.pathom.core :as p]
            [ring.middleware.anti-forgery :refer [*anti-forgery-token*]]
            [clojure.spec.alpha :as s]
            [fulcro.server :as server :refer [parser server-mutate defmutation]]))

(defn guard-attributes
  [{:keys [::p/sql-schema] :as env}]
  (let [{:keys []} sql-schema
        e          (p/entity env)
        k          (get-in env [:ast :dispatch-key])]
    (if (contains? #{:person/secret :pet/private} k)
      ::p/forbidden
      ::p/continue)))

(def derive-attributes
  {:pet/age
   (fn [env]
     (let [{:pet/keys [yob]} (p/entity env)]
       (- 2018 yob)))
   :person/age
   (fn [env]
     (let [{:person/keys [yob]} (p/entity env)]
       (- 2018 yob)))})

(def pathom-parser
  (p/parser
    {:mutate server-mutate
     ::p/plugins
     [(p/env-plugin
        {::p/reader
         [sqb/pull-entities guard-attributes derive-attributes p/map-reader]})]}))

(defmethod ig/init-key ::sql-schema [_ schema]
  ;; todo: advanced schema here
  (sqb/compile-schema schema))

(defmethod ig/init-key ::run-query [_ _params]
  (fn kk [db query]
    (jdbc/with-db-connection [conn (:spec db)]
      (jdbc/query conn query))))

(defmethod ig/init-key ::fulcro [_ {:keys [:duct/logger] :as env}]
  (fn [request]
    {:body (pathom-parser env (:body-params request))}))
