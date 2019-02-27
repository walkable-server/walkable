(ns walkable.sql-query-builder.pagination
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [walkable.sql-query-builder.expressions :as expressions]
            [clojure.set :as set]))

(defn column+order-params-spec
  [allowed-keys]
  (s/+
    (s/cat
      :column ::expressions/namespaced-keyword
      :params (s/* allowed-keys))))

(defn ->conform-order-by
  [allowed-keys]
  (fn conform-order-by [order-by]
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
      (fn wrapped-validate-order-by [conformed-order-by]
        (when-not (s/invalid? conformed-order-by)
          (every? f (map :column conformed-order-by))))
      identity)))

(defn order-by-fallback*
  [{:keys [conform stringify]}
   {:keys [default validate throw?]}]
  (let [default  (when default
                   (let [conformed (conform default)]
                     (assert (not (s/invalid? conformed))
                       "Malformed default value")
                     (columns-and-string conformed stringify)))
        validate (wrap-validate-order-by validate)]
    (if throw?
      (fn aggressive-fallback [supplied]
        (let [conformed (conform supplied)]
          (if (s/invalid? conformed)
            (throw (ex-info "Malformed!" {}))
            (if (validate conformed)
              (columns-and-string conformed stringify)
              (throw (ex-info "Invalid!" {}))))))
      (fn silent-fallback [supplied]
        (let [conformed (conform supplied)]
          (if (and (not (s/invalid? conformed)) (validate conformed))
            (columns-and-string conformed stringify)
            default))))))

(defn order-by-fallback
  [{:keys [conform-order-by stringify-order-by] :as emitter}
   clojuric-names order-by-config]
  (order-by-fallback*
    {:conform   conform-order-by
     :stringify #(stringify-order-by clojuric-names %)}
    order-by-config))

(defn number-fallback
  [{:keys [stringify conform wrap-validate]}
   {:keys [default validate throw?] :or {validate (constantly true)}}]
  (let [default
        (when default
          (let [conformed (conform default)]
            (assert (not (s/invalid? conformed))
              "Malformed default value")
            (stringify conformed)))
        validate (wrap-validate validate)]
    (if throw?
      (fn aggressive-fallback [supplied]
        (let [conformed (conform supplied)]
          (if (s/invalid? conformed)
            (throw (ex-info "Malformed!" {}))
            (if (validate conformed)
              (stringify conformed)
              (throw (ex-info "Invalid!" {}))))))
      (fn silent-fallback [supplied]
        (let [conformed (conform supplied)
              valid?    (and (not (s/invalid? conformed))
                          (validate conformed))]
          (if valid?
            (stringify conformed)
            default))))))

(defn offset-fallback
  [emitter offset-config]
  (number-fallback {:stringify     (:stringify-offset emitter)
                    :conform       (:conform-offset emitter)
                    :wrap-validate (:wrap-validate-offset emitter)}
    offset-config))

(defn limit-fallback
  [emitter limit-config]
  (number-fallback {:stringify     (:stringify-limit emitter)
                    :conform       (:conform-limit emitter)
                    :wrap-validate (:wrap-validate-limit emitter)}
    limit-config))

(defn compile-fallbacks*
  [emitter clojuric-names pagination-fallbacks]
  (reduce (fn [acc [k {offset-config   :offset
                       limit-config    :limit
                       order-by-config :order-by}]]
            (let [v {:offset-fallback
                     (offset-fallback emitter offset-config)

                     :limit-fallback
                     (limit-fallback emitter limit-config)

                     :order-by-fallback
                     (order-by-fallback emitter clojuric-names order-by-config)}]
              (assoc acc k v)))
    {}
    pagination-fallbacks))

(defn compile-fallbacks
  [emitter clojuric-names pagination-fallbacks]
  (->> (clojure.set/rename-keys pagination-fallbacks {:default `default-fallbacks})
    (merge {`default-fallbacks {:offset   {}
                                :limit    {}
                                :order-by {}}})
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
