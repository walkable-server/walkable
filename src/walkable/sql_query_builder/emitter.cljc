(ns walkable.sql-query-builder.emitter)

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

(defn table-name [this k]
  (let [t (namespace k)]
    (->> (clojure.string/split t #"\.")
      (map #(with-quote-marks this
              (or (get (:rename-tables this) %)
                (transform-table-name this %))))
      (clojure.string/join "."))))

(defn column-name [this k]
  (str (table-name this k) "."
    (let [c (name k)]
      (or (get (:rename-columns this) c)
        (transform-table-name this c)))))

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
   :wrap-select-strings   ["(" ")"]})

(def sqlite-emitter
  (merge default-emitter
    {:wrap-select-strings ["SELECT * FROM (" ")"]}))

(def postgres-emitter
  default-emitter)

(def mysql-emitter
  (merge default-emitter
    {:quote-marks backticks}))

(comment
  (with-quote-marks sqlite-emitter "abc")
  (column-name sqlite-emitter :abc.def/xyz)
  (column-name sqlite-emitter :abc.def/xyz-tuv)
  (table-name sqlite-emitter :abc.def/xyz-tuv))
