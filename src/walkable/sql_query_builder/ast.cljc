(ns walkable.sql-query-builder.ast
  (:require [clojure.zip :as z]
            [clojure.spec.alpha :as s]))

(defn ast-root
  [ast]
  (assoc ast ::my-marker :root))

(defn ast-zipper-root?
  [x]
  (= :root (::my-marker x)))

(s/def ::zipper-fns
  (s/keys :req-un [::placeholder? ::leaf?]))

(defn ast-zipper
  "Make a zipper to navigate an ast tree possibly with placeholder
  subtrees."
  [ast {:keys [placeholder? leaf?] :as zipper-fns}]
  {:pre [(map? ast) (s/valid? ::zipper-fns zipper-fns)]}
  (->> ast
    (z/zipper
      (fn branch? [x] (and (map? x)
                        (or (ast-zipper-root? x) (placeholder? x))
                        (seq (:children x))))
      (fn children [x] (->> (:children x) (filter #(or (leaf? %) (placeholder? %)))))
      ;; not neccessary because we only want to read, not write
      (fn make-node [x xs] (assoc x :children (vec xs))))))

(defn all-zipper-children
  "Given a zipper, returns all its children"
  [zipper]
  (->> zipper
    (iterate z/next)
    (take-while #(not (z/end? %)))))

(defn find-all-children
  "Find all direct children, or children in nested placeholders."
  [ast {:keys [placeholder? leaf?] :as zipper-fns}]
  {:pre [(map? ast) (s/valid? ::zipper-fns zipper-fns)]}
  (->>
    (ast-zipper (ast-root ast)
      {:leaf?        leaf?
       :placeholder? placeholder?})
    (all-zipper-children)
    (map z/node)
    (remove #(or (ast-zipper-root? %) (placeholder? %)))))
