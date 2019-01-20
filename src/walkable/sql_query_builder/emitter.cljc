(ns walkable.sql-query-builder.emitter
  (:require [clojure.spec.alpha :as s]
            [walkable.sql-query-builder.expressions :as expressions]))

(def backticks
  (repeat 2 "`"))

(def quotation-marks
  (repeat 2 "\""))

(def apostrophes
  (repeat 2 "'"))

(defn dash-to-underscore [s]
  (clojure.string/replace s #"-" "_"))

(defn with-quote-marks [this s]
  (let [[quote-open quote-close] (:quote-marks this)]
    (str quote-open s quote-close)))

(defn transform-table-name [this table-name]
  ((:transform-table-name this) table-name))

(defn transform-column-name [this column-name]
  ((:transform-column-name this) column-name))

(defn table-name* [this t]
  (->> (clojure.string/split t #"\.")
    (map #(with-quote-marks this
            (or (get (:rename-tables this) %)
              (transform-table-name this %))))
    (clojure.string/join ".")))

(defn true-keyword [this k]
  ((:rename-keywords this) k k))

(defn table-name [this k]
  (let [t (namespace (true-keyword this k))]
    (table-name* this t)))

(defn column-name [this k]
  (let [k (true-keyword this k)]
    (str (table-name this k) "."
      (let [c (name k)]
        (with-quote-marks this
          (or (get (:rename-columns this) c)
            (transform-table-name this c)))))))

(defn clojuric-name [this k]
  (with-quote-marks this (subs (str k) 1)))

(defn wrap-select [this s]
  (let [[wrap-open wrap-close] (:wrap-select-strings this)]
    (str wrap-open s wrap-close)))

(def default-emitter
  {:quote-marks           quotation-marks
   :transform-table-name  dash-to-underscore
   :transform-column-name dash-to-underscore
   :rename-tables         {}
   :rename-columns        {}
   :rename-keywords       {}
   :wrap-select-strings   ["(" ")"]})

(def sqlite-emitter
  (merge default-emitter
    {:wrap-select-strings ["SELECT * FROM (" ")"]}))

(def postgres-emitter
  default-emitter)

(def mysql-emitter
  (merge default-emitter
    {:quote-marks backticks}))

(s/def ::query-string-input
  (s/keys :req-un [::selection ::target-table]
    :opt-un [::join-statement ::conditions
             ::offset ::limit ::order-by]))

(defn ->query-string
  "Builds the final query string ready for SQL server."
  [{:keys [selection target-table join-statement conditions
           offset limit order-by]
    :as input}]
  {:pre  [(s/valid? ::query-string-input input)]
   :post [string?]}
  (str "SELECT " selection
    " FROM " target-table

    join-statement

    (when conditions
      (str " WHERE "
        conditions))

    offset
    limit
    order-by))

(defn emitter->batch-query [emitter]
  (fn [parameterized-queries]
    (-> (if (= 1 (count parameterized-queries))
          (first parameterized-queries)
          (expressions/concatenate
            (fn [q] (->> q
                      (mapv #(wrap-select emitter %))
                      (clojure.string/join "\nUNION ALL\n")))
            parameterized-queries)))))

(comment
  (= ((emitter->batch-query default-emitter)
      [{:raw-string "x" :params ["a" "b"]}
       {:raw-string "y" :params ["c" "d"]}])
    {:params     ["a" "b" "c" "d"],
     :raw-string "(x)\nUNION ALL\n(y)"})
  (= ((emitter->batch-query sqlite-emitter)
      [{:raw-string "x" :params ["a" "b"]}
       {:raw-string "y" :params ["c" "d"]}])
    {:params     ["a" "b" "c" "d"],
     :raw-string "SELECT * FROM (x)\nUNION ALL\nSELECT * FROM (y)"}))
