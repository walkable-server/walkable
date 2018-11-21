(ns walkable.sql-query-builder.pagination
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [walkable.sql-query-builder.expressions :as expressions]
            [clojure.set :as set]))

(defn merge-pagination [{:keys [offset-fallback limit-fallback order-by-fallback]
                         :or   {offset-fallback   identity
                                limit-fallback    identity
                                order-by-fallback identity}}
                        {:keys [offset limit order-by]}]
  {:offset   (offset-fallback offset)
   :limit    (limit-fallback limit)
   :order-by (order-by-fallback order-by)})

(defn wrap-validate-number [f]
  (if (ifn? f)
    #(and (number? %) (f %))
    #(number? %)))

(s/def ::column+order-params
  (s/cat
    :column ::expressions/namespaced-keyword
    :params (s/* #{:asc :desc :nils-first :nils-last})))

(defn order-by-columns [order-by]
  (let [form (s/conform (s/+ ::column+order-params) order-by)]
    (when-not (= ::s/invalid form)
      (map :column form))))

(defn wrap-validate-order-by [f]
  (comp boolean
    (if (ifn? f)
      #(when-let [xs (order-by-columns %)]
         (every? f xs))
      #(order-by-columns %))))

(defn fallback [wrap-validate {:keys [default validate]}]
  (fn [supplied]
    (if ((wrap-validate validate) supplied)
      supplied
      default)))

(def offset-fallback #(fallback wrap-validate-number %))
(def limit-fallback offset-fallback)

(def order-by-fallback #(fallback wrap-validate-order-by %))

(def order-params->string
  {:asc        " ASC"
   :desc       " DESC"
   :nils-first " NULLS FIRST"
   :nils-last  " NULLS LAST"})

(defn ->order-by-string [column-names order-by]
  (let [form (s/conform (s/+ ::column+order-params) order-by)]
    (when-not (= ::s/invalid form)
      (let [form (filter #(contains? column-names (:column %)) form)]
        (when (seq form)
          (->> form
            (map (fn [{:keys [column params]}]
                   (str
                     (get column-names column)
                     (->> params
                       (map order-params->string)
                       (apply str)))))
            (clojure.string/join ", ")))))))

(defn stringify-order-by [column-names m]
  (update m :order-by #(->order-by-string column-names %)))
