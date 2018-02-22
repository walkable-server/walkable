(ns walkable-demo.ui.components
  (:require
    [fulcro.client.dom :as dom]
    [fulcro.client.primitives :as prim :refer [defsc]]))

(defsc Person [this {:person/keys [number name age]}]
  {:query         [:person/number :person/name :person/age]
   :ident         [:person/by-id :person/number]
   :initial-state (fn [{:keys [number name age]}] #:person{:number number :name name :age age})}
  (dom/li nil
    (dom/h5 nil name (str "(age: " age ")"))))

(def ui-person (prim/factory Person {:keyfn :person/number}))

(defsc Root [this {:keys [ui/react-key]}]
  {:query [:ui/react-key {[:person/by-id 1] (prim/get-query Person)}]
   :initial-state {}}
  (dom/div #js {:key react-key}
    "Hello world"
    (ui-person {:person/number 1})))
