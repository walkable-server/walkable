(ns walkable-demo.ui.components
  (:require
    [fulcro.client.dom :as dom]
    [fulcro.client.primitives :as prim :refer [defsc]]))

(defsc Pet [this {:pet/keys [index age color]}]
  {:query         [:pet/index :pet/age :pet/color]
   :ident         [:pet/by-id :pet/index]
   :initial-state (fn [{:pet/keys [index age color]}]
                    #:pet{:index index :color color :age age})}
  (dom/ul nil
    (dom/li nil
      (dom/h5 nil
        "<" color ", " age " years old>"))))

(def ui-pet (prim/factory Pet {:keyfn :pet/index}))

(defsc Person [this {:person/keys [number name age pet]}]
  {:query         [:person/number :person/name :person/age
                   {:person/pet (prim/get-query Pet)}]
   :ident         [:person/by-id :person/number]
   :initial-state (fn [{:person/keys [number name age pet]}]
                    #:person{:number number :name name :age age
                             :pet pet})}
  (dom/ul nil
    (dom/li nil
      (dom/h5 nil name (str "(age: " age ")"))
      (map ui-pet pet))))

(def ui-person (prim/factory Person {:keyfn :person/number}))

(defsc Root [this {:keys [ui/react-key]}]
  {:query         [:ui/react-key]
   :initial-state {}}
  (dom/div #js {:key react-key}
    "Hello world"
    (ui-person {:person/number 1})))
