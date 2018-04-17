(ns walkable.sql-query-builder.pagination
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [walkable.sql-query-builder.filters :as filters]
            [clojure.set :as set]))

(s/def ::column+order-params
  (s/cat
    :column ::filters/namespaced-keyword
    :params (s/* #{:asc :desc :nils-first :nils-last})))

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
