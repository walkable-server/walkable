(ns walkable-demo.middleware.fulcro
  (:require [ataraxy.core :as ataraxy]
            [ataraxy.response :as response]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [org.httpkit.server]
            [ring.middleware.content-type :refer [wrap-content-type]]
            [fulcro.server :as server]))
