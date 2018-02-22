(ns walkable-demo.client
  (:require
   [fulcro.client.data-fetch :as df]
   [devcards.core :as dc :refer-macros [defcard]]
   [fulcro.client.network :as network]
   [fulcro.client.cards :refer [defcard-fulcro]]
   [walkable-demo.ui.components :as comp]
   [sablono.core :as sab]))

(defcard-fulcro trip-app
  "Some doc"
  comp/Root
  {}
  {:inspect-data true
   :fulcro {:started-callback
            (fn [app] (df/load app [:person/by-id 1] comp/Person))}})

(dc/start-devcard-ui!)
