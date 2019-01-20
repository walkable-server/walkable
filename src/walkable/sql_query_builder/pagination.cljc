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

(defn wrap-validate-order-by [f]
  (comp boolean
    (if (ifn? f)
      (fn [conformed-order-by]
        (when conformed-order-by
          (every? f (map :column conformed-order-by))))
      identity)))

(defn fallback [{:keys [wrap-validate stringify conform]} {:keys [default validate]}]
  (when default
    (let [default  (stringify (if (fn? conform)
                                (conform default)
                                default))
          validate (wrap-validate validate)]
      (if (fn? conform)
        (fn [supplied]
          (let [conformed (conform supplied)]
            (if (and conformed (validate conformed))
              (stringify conformed)
              default)))
        (fn [supplied]
          (if (validate supplied)
            (stringify supplied)
            default))))))

(defn offset-fallback
  [offset]
  (fallback {:wrap-validate wrap-validate-number
             :stringify     #(when % (str " OFFSET " %))}
    offset))

(defn limit-fallback
  [limit]
  (fallback {:wrap-validate wrap-validate-number
             :stringify     #(when % (str " LIMIT " %))}
    limit))

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

(defn order-by-fallback
  [clojuric-names order-by]
  (fallback {:wrap-validate wrap-validate-order-by
             :conform       #(conform-order-by clojuric-names %)
             :stringify     #(when % (str " ORDER BY "
                                       (->order-by-string clojuric-names %)))}
    order-by))

(defn compile-fallbacks
  [clojuric-names pagination-fallbacks]
  (reduce (fn [acc [k {:keys [offset limit order-by]}]]
            (let [v {:offset-fallback
                     (offset-fallback offset)

                     :limit-fallback
                     (limit-fallback limit)

                     :order-by-fallback
                     (order-by-fallback clojuric-names order-by)}]
              (assoc acc k v)))
    {}
    pagination-fallbacks))

(defn merge-pagination
  [clojuric-names
   {:keys [offset-fallback limit-fallback order-by-fallback]}
   {:keys [offset limit order-by]}]
  (let [offset-fallback   (or offset-fallback identity)
        limit-fallback    (or limit-fallback identity)
        order-by-fallback (or order-by-fallback identity)]
    {:offset   (offset-fallback offset)
     :limit    (limit-fallback limit)
     :order-by (order-by-fallback order-by)}))

(defn process-pagination
  [clojuric-names supplied-pagination pagination-fallbacks]
  (->> supplied-pagination
    (add-conformed-order-by clojuric-names)
    (merge-pagination pagination-fallbacks)
    (add-order-by-columns)
    (stringify-order-by clojuric-names)))
