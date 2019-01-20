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

(defn number-fallback [{:keys [stringify]} {:keys [default validate]}]
  (let [default  (when default (stringify default))
        validate (wrap-validate-number validate)]
    (fn [supplied]
      (if (validate supplied)
        (stringify supplied)
        default))))

(defn offset-fallback
  [offset]
  (number-fallback {:stringify #(when % (str " OFFSET " %))}
    offset))

(defn limit-fallback
  [limit]
  (number-fallback {:stringify #(when % (str " LIMIT " %))}
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

(defn columns-and-string
  [conformed stringify]
  {:columns (into #{} (map :column) conformed)
   :string  (stringify conformed)})

(defn order-by-fallback*
  [{:keys [conform stringify]}
   {:keys [default validate]}]
  (let [default  (when default
                   (let [conformed (conform default)]
                     (columns-and-string conformed stringify)))
        validate (wrap-validate-order-by validate)]
    (fn [supplied]
      (let [conformed (conform supplied)]
        (if (and conformed (validate conformed))
          (columns-and-string conformed stringify)
          default)))))

(defn order-by-fallback
  [clojuric-names order-by]
  (order-by-fallback*
    {:conform       #(conform-order-by clojuric-names %)
     :stringify     #(when % (str " ORDER BY "
                               (->order-by-string clojuric-names %)))}
    order-by))

(defn compile-fallbacks*
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

(defn compile-fallbacks
  [clojuric-names pagination-fallbacks]
  (->> (assoc pagination-fallbacks
         `default-fallbacks {:offset   {}
                             :limit    {}
                             :order-by {}})
    (compile-fallbacks* clojuric-names)))

(defn merge-pagination
  [default-fallbacks
   {:keys [offset-fallback limit-fallback order-by-fallback]}
   {:keys [offset limit order-by]}]
  (let [offset-fallback   (or offset-fallback (get default-fallbacks :offset-fallback))
        limit-fallback    (or limit-fallback (get default-fallbacks :limit-fallback))
        order-by-fallback (or order-by-fallback (get default-fallbacks :order-by-fallback))

        {:keys [string columns]} (order-by-fallback order-by)]
    {:offset           (offset-fallback offset)
     :limit            (limit-fallback limit)
     :order-by         string
     :order-by-columns columns}))
