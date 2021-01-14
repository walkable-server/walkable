(ns walkable.sql-query-builder.expressions-test
  (:require [walkable.sql-query-builder.expressions :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest inline-params-tests
  (= (sut/inline-params {}
       {:raw-string "? + 1"
        :params     [{:raw-string "2018 - `human`.`yob` + ?"
                      :params     [(sut/av :a/b)]}]})
    {:raw-string "2018 - `human`.`yob` + ? + 1"
     :params     [(sut/av :a/b)]})
  (is (= (sut/inline-params {}
           {:raw-string "?"
            :params     [{:raw-string "2018 - `human`.`yob`"
                          :params     []}]})
        {:raw-string "2018 - `human`.`yob`"
         :params     []}))
  (is (= (sut/inline-params {}
           {:raw-string " ?"
            :params     [{:raw-string "2018 - `human`.`yob`"
                          :params     []}]})
        {:raw-string " 2018 - `human`.`yob`",
         :params     []}))
  (is (= (sut/inline-params {}
           {:raw-string "? "
            :params     [{:raw-string "2018 - `human`.`yob`"
                          :params     []}]})
        {:raw-string "2018 - `human`.`yob` ",
         :params []})))

(deftest concatenate-params-tests
  (is (= (sut/concatenate #(apply str %)
           [{:raw-string "? as a"
             :params     [(sut/av :a/b)]}
            {:raw-string "? as b"
             :params     [(sut/av :c/d)]}])
        {:params     [(sut/av :a/b) (sut/av :c/d)],
         :raw-string "? as a? as b"}))
  (is (= (sut/concatenate #(clojure.string/join ", " %)
           [{:raw-string "? as a"
             :params     [(sut/av :a/b)]}
            {:raw-string "? as b"
             :params     [(sut/av :c/d)]}])
        {:params [(sut/av :a/b) (sut/av :c/d)],
         :raw-string "? as a, ? as b"})))

(deftest compile-to-string-tests
  (is (= (sut/compile-to-string {:join-filter-subqueries
                                 {:x/a "x.a_id IN (SELECT a.id FROM a WHERE ?)"
                                  :x/b "x.id IN (SELECT x_b.x_id FROM x_b JOIN b ON b.id = x_b.b_id WHERE ?)"}}
           [:or {:x/a [:= :a/foo "meh"]}
            {:x/b [:= :b/bar "mere"]}])
        {:params     [(sut/av :a/foo) "meh" (sut/av :b/bar) "mere"]
         :raw-string "((x.a_id IN (SELECT a.id FROM a WHERE (?)=(?)))) OR ((x.id IN (SELECT x_b.x_id FROM x_b JOIN b ON b.id = x_b.b_id WHERE (?)=(?))))"})))

(deftest substitute-atomic-variables-test
  (is (= (->> (sut/compile-to-string {} [:= :x/a "abc" [:+ 24 [:+ :x/b 2]]])
           (sut/substitute-atomic-variables {:variable-values {:x/a  {:raw-string "?" :params ["def"]}}})
           (sut/substitute-atomic-variables {:variable-values {:x/b  (sut/compile-to-string {} [:+ 2018 "713"])}}))
        {:params ["def" "abc" "abc" "713"], :raw-string "(?)=(?) AND (?)=((24)+(((2018)+(?))+(2)))"})))
