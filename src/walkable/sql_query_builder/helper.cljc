(ns walkable.sql-query-builder.helper
  (:require [weavejester.dependency :as dep]))

(defn check-circular-dependency!
  [graph]
  (try
    (reduce (fn [acc [x y]] (dep/depend acc y x))
      (dep/graph)
      graph)
    (catch Exception e
      (let [{:keys [node dependency] :as data} (ex-data e)]
        (throw (ex-info (str "Circular dependency between " node " and " dependency)
                 data))))))

(defn build-index [k coll]
  (into {} (for [o coll] [(get o k) o])))

(defn build-index-of [k coll]
  (into {} (for [x coll
                 :let [i (:key x)
                       j (get x k)]]
             [i j])))
