(ns walkable.sql-query-builder.pagination
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [walkable.sql-query-builder.expressions :as expressions]
            [clojure.set :as set]))

(defn wrap-validate-number [f]
  (if (ifn? f)
    #(and (number? %) (f %))
    #(number? %)))

(defn column+order-params-spec
  [allowed-keys]
  (s/+
    (s/cat
      :column ::expressions/namespaced-keyword
      :params (s/* allowed-keys))))

(defn ->conform-order-by
  [allowed-keys]
  (fn [order-by]
    (let [order-by (if (sequential? order-by) order-by [order-by])]
      (s/conform (column+order-params-spec allowed-keys) order-by))))

(defn ->stringify-order-by
  [order-params->string]
  (fn stringify-order-by [clojuric-names conformed-order-by]
    (when conformed-order-by
      (->> conformed-order-by
        (map (fn [{:keys [column params]}]
               (str
                 (get clojuric-names column)
                 (->> params
                   (map order-params->string)
                   (apply str)))))
        (clojure.string/join ", ")
        (str " ORDER BY ")))))

(defn columns-and-string
  [conformed stringify]
  {:columns (into #{} (map :column) conformed)
   :string  (stringify conformed)})

(defn wrap-validate-order-by [f]
  (comp boolean
    (if (ifn? f)
      (fn [conformed-order-by]
        (when-not (s/invalid? conformed-order-by)
          (every? f (map :column conformed-order-by))))
      identity)))

(defn number-fallback
  [{:keys [stringify conform]}
   {:keys [default validate]}]
  (let [default  (when default (stringify default))
        validate (wrap-validate-number validate)]
    (fn [supplied]
      (let [conformed (conform supplied)
            v?        (validate conformed)]
        (if v?
          (stringify conformed)
          default)))))

(defn emitter->offset-fallback
  [emitter]
  (fn [offset]
    (number-fallback {:stringify (:stringify-offset emitter)
                      :conform   (:conform-offset emitter)}
      offset)))

(defn emitter->limit-fallback
  [emitter]
  (fn [limit]
    (number-fallback {:stringify (:stringify-limit emitter)
                      :conform   (:conform-limit emitter)}
      limit)))
(defn order-by-fallback*
  [{:keys [conform stringify]}
   {:keys [default validate]}]
  (let [default  (when default
                   (let [conformed (conform default)]
                     (assert (not (s/invalid? conformed)))
                     (columns-and-string conformed stringify)))
        validate (wrap-validate-order-by validate)]
    (fn [supplied]
      (let [conformed (conform supplied)]
        (if (and (not (s/invalid? conformed)) (validate conformed))
          (columns-and-string conformed stringify)
          default)))))

(defn order-by-fallback
  [{:keys [conform-order-by stringify-order-by] :as emitter}
   clojuric-names order-by-config]
  (order-by-fallback*
    {:conform   conform-order-by
     :stringify #(stringify-order-by clojuric-names %)}
    order-by-config))


(defn compile-fallbacks*
  [emitter clojuric-names pagination-fallbacks]
  (reduce (fn [acc [k {:keys [offset limit order-by]}]]
            (let [v {:offset-fallback
                     ((emitter->offset-fallback emitter) offset)

                     :limit-fallback
                     ((emitter->limit-fallback emitter) limit)

                     :order-by-fallback
                     (order-by-fallback clojuric-names order-by)}]
              (assoc acc k v)))
    {}
    pagination-fallbacks))

(defn compile-fallbacks
  [emitter clojuric-names pagination-fallbacks]
  (->> (merge {`default-fallbacks {:offset   {}
                                   :limit    {}
                                   :order-by {}}}
         pagination-fallbacks)
    (compile-fallbacks* emitter clojuric-names)))

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
