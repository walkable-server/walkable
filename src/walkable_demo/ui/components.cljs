(ns walkable-demo.ui.components
  (:require
    [fulcro.client.dom :as dom]
    [fulcro.client.primitives :as prim :refer [defsc]]))

(defsc Person [this {:person/keys [number name age]} {:keys [onDelete]}]
  {:query         [:person/number :person/name :person/age]
   :ident         [:person/by-id :person/number]
   :initial-state (fn [{:keys [number name age]}] {:person/number number :person/name name :person/age age})}
  (dom/li nil
    (dom/h5 nil name (str "(age: " age ")") (dom/button #js {} "X"))))

(def ui-person (prim/factory Person))

(defsc Root [this {:keys [ui/react-key]}]
  {:initial-state (fn [params] {:xs []})}
  (dom/div #js {:key react-key}
    "Hello world"
    (ui-person {:person/number 1})))
