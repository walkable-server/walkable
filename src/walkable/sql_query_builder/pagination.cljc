(ns walkable.sql-query-builder.pagination
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [walkable.sql-query-builder.expressions :as expressions]
            [clojure.set :as set]))

(defn wrap-validate-number [f]
  (if (ifn? f)
    #(and (number? %) (f %))
    #(number? %)))

(s/def ::column+order-params
  (s/cat
    :column ::expressions/namespaced-keyword
    :params (s/* #{:asc :desc :nils-first :nils-last})))

(defn conform-order-by [clojuric-names order-by]
  (let [form (s/conform (s/+ ::column+order-params) order-by)]
    (when-not (= ::s/invalid form)
      (let [form (filter #(contains? clojuric-names (:column %)) form)]
        (when (seq form)
          (vec form))))))

(defn conform-fallback-default [clojuric-names fallback]
  (update fallback :default #(conform-order-by clojuric-names %)))

(defn wrap-validate-order-by [f]
  (comp boolean
    (if (ifn? f)
      (fn [conformed-order-by]
        (when conformed-order-by
          (every? f (map :column conformed-order-by))))
      identity)))

(defn fallback [wrap-validate {:keys [default validate]}]
  (when default
    (fn [supplied]
      (if ((wrap-validate validate) supplied)
        supplied
        default))))

(def offset-fallback #(fallback wrap-validate-number %))
(def limit-fallback offset-fallback)

(def order-by-fallback #(fallback wrap-validate-order-by %))

(def order-params->string
  {:asc        " ASC"
   :desc       " DESC"
   :nils-first " NULLS FIRST"
   :nils-last  " NULLS LAST"})

(defn ->order-by-string [clojuric-names conformed-order-by]
  (when conformed-order-by
    (->> conformed-order-by
      (map (fn [{:keys [column params]}]
             (str
               (get clojuric-names column)
               (->> params
                 (map order-params->string)
                 (apply str)))))
      (clojure.string/join ", "))))

(defn add-conformed-order-by [clojuric-names {:keys [order-by] :as m}]
  (-> m
    (assoc :conformed-order-by (conform-order-by clojuric-names order-by))
    (dissoc :order-by)))

(defn add-order-by-columns [{:keys [conformed-order-by] :as m}]
  (assoc m :order-by-columns (into #{} (map :column) conformed-order-by)))

(defn stringify-order-by [clojuric-names {:keys [conformed-order-by] :as m}]
  (-> m
    (assoc :order-by (->order-by-string clojuric-names conformed-order-by))
    (dissoc :conformed-order-by)))

(defn merge-pagination [{:keys [offset-fallback limit-fallback order-by-fallback]}
                        {:keys [offset limit conformed-order-by]}]
  (let [offset-fallback   (or offset-fallback identity)
        limit-fallback    (or limit-fallback identity)
        order-by-fallback (or order-by-fallback identity)]
    {:offset             (offset-fallback offset)
     :limit              (limit-fallback limit)
     :conformed-order-by (order-by-fallback conformed-order-by)}))

(defn process-pagination
  [clojuric-names supplied-pagination pagination-fallbacks]
  (->> supplied-pagination
    (add-conformed-order-by clojuric-names)
    (add-order-by-columns)
    (merge-pagination pagination-fallbacks)
    (stringify-order-by clojuric-names)))
