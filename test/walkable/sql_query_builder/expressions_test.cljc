(ns walkable.sql-query-builder.expressions-test
  (:require [walkable.sql-query-builder.expressions :as sut]
            [walkable.sql-query-builder.helper :as helper]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
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
  (is (= (sut/concatenate #(string/join ", " %)
           [{:raw-string "? as a"
             :params     [(sut/av :a/b)]}
            {:raw-string "? as b"
             :params     [(sut/av :c/d)]}])
        {:params [(sut/av :a/b) (sut/av :c/d)],
         :raw-string "? as a, ? as b"})))

(deftest compile-to-string-tests
  (is (= (sut/compile-to-string {:operators (helper/build-index :key sut/common-operators)
                                 :join-filter-subqueries
                                 {:x/a "x.a_id IN (SELECT a.id FROM a WHERE ?)"
                                  :x/b "x.id IN (SELECT x_b.x_id FROM x_b JOIN b ON b.id = x_b.b_id WHERE ?)"}}
           [:or {:x/a [:= :a/foo "meh"]}
            {:x/b [:= :b/bar "mere"]}])
        {:params     [(sut/av :a/foo) "meh" (sut/av :b/bar) "mere"]
         :raw-string "((x.a_id IN (SELECT a.id FROM a WHERE (?)=(?)))) OR ((x.id IN (SELECT x_b.x_id FROM x_b JOIN b ON b.id = x_b.b_id WHERE (?)=(?))))"}))
  (is (= (->> [:cast :a/c :text]
           (sut/compile-to-string {:operators (helper/build-index :key sut/common-operators)}))
        {:params [(sut/av :a/c)]
         :raw-string "CAST (? AS text)"})))

(deftest substitute-atomic-variables-test
  (is (= (let [registry {:operators (helper/build-index :key sut/common-operators)}]
           (->> [:= :x/a "abc" [:+ 24 [:+ :x/b 2]]]
             (sut/compile-to-string registry)
             (sut/substitute-atomic-variables {:variable-values {:x/a  {:raw-string "?" :params ["def"]}}})
             (sut/substitute-atomic-variables {:variable-values {:x/b  (sut/compile-to-string registry [:+ 2018 "713"])}})))
        {:params ["def" "abc" "abc" "713"], :raw-string "(?)=(?) AND (?)=((24)+(((2018)+(?))+(2)))"})))
