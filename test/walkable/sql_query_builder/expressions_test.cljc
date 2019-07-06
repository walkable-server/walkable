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

(deftest and-tests
  (is (= (sut/process-operator {} [:and []])
        {:raw-string "?", :params [true]}))
  (is (= (sut/process-operator {} [:and [{}]])
        {:raw-string "(?)", :params [{}]}))
  (is (= (sut/process-operator {} [:and [{} {}]])
        {:raw-string "(?) AND (?)", :params [{} {}]}))
  (is (= (sut/process-operator {} [:and [{} {} {}]])
        {:raw-string "(?) AND (?) AND (?)", :params [{} {} {}]})))

(deftest or-tests
  (is (= (sut/process-operator {} [:or []])
        {:raw-string "?", :params [false]}))
  (is (= (sut/process-operator {} [:or [{}]])
        {:raw-string "(?)", :params [{}]}))
  (is (= (sut/process-operator {} [:or [{} {}]])
        {:raw-string "(?) OR (?)", :params [{} {}]}))
  (is (= (sut/process-operator {} [:or [{} {} {}]])
        {:raw-string "(?) OR (?) OR (?)", :params [{} {} {}]})))

(deftest in-tests
  (is (thrown-with-msg? #?(:clj java.lang.AssertionError :cljs js/Error)
        #"There must be at least two parameters"
        (sut/process-operator {} [:in []])))
  (is (thrown-with-msg? #?(:clj java.lang.AssertionError :cljs js/Error)
        #"There must be at least two parameters"
        (sut/process-operator {} [:in [{}]])))
  (is (= (sut/process-operator {} [:in [{} {}]])
        {:raw-string "(?) IN (?)", :params [{} {}]}))
  (is (= (sut/process-operator {} [:in [{} {} {}]])
        {:raw-string "(?) IN (?, ?)", :params [{} {} {}]})))

(deftest not-tests
  (is (= (sut/process-operator {} [:not [{}]])
        {:raw-string "NOT(?)", :params [{}]})))

(deftest nil?-tests
  (is (= (sut/process-operator {} [:nil? [{}]])
        {:raw-string "(?) is null", :params [{}]})))

(deftest =-tests
  ;; the same to >, >=, <, <=
  (is (= (sut/process-operator {} [:= [{} {}]])
        {:raw-string "(?)=(?)", :params [{} {}]}))
  (is (= (sut/process-operator {} [:= [{} {} {}]])
        {:raw-string "(?)=(?) AND (?)=(?)", :params [{} {} {} {}]}))
  (is (= (sut/process-operator {} [:= [{} {} {} {}]])
        {:raw-string "(?)=(?) AND (?)=(?) AND (?)=(?)", :params [{} {} {} {} {} {}]})))

(deftest +-and-the-like-tests
  (testing "special case for * and +"
    (is (= (sut/process-operator {} [:* []])
            {:raw-string "1", :params []}))
    (is (= (sut/process-operator {} [:+ []])
          {:raw-string "0", :params []})))

  (testing "special case for - and /"
    (is (= (sut/process-operator {} [:- [{}]])
          {:raw-string "0-(?)", :params [{}]}))
    (is (= (sut/process-operator {} [:/ [{}]])
          {:raw-string "1/(?)", :params [{}]})))

  (testing "normal case for + (the same to *, -, /)"
    (is (= (sut/process-operator {} [:+ [{}]])
          {:raw-string "(?)", :params [{}]}))
    (is (= (sut/process-operator {} [:+ [{} {}]])
          {:raw-string "(?)+(?)", :params [{} {}]}))
    (is (= (sut/process-operator {} [:+ [{} {} {}]])
          {:raw-string "(?)+(?)+(?)", :params [{} {} {}]}))))

(deftest when-test
  (is (= (sut/process-operator {} [:when [{} {}]])
        {:raw-string "CASE WHEN (?) THEN (?) END", :params [{} {}]})))

(deftest if-test
  (is (= (sut/process-operator {} [:if [{} {}]])
        {:raw-string "CASE WHEN (?) THEN (?) END", :params [{} {}]}))
  (is (= (sut/process-operator {} [:if [{} {} {}]])
        {:raw-string "CASE WHEN (?) THEN (?) ELSE (?) END", :params [{} {} {}]})))

(deftest case-test
  (is (= (sut/process-operator {} [:case [{} {} {}]])
        {:raw-string "CASE (?) WHEN (?) THEN (?) END", :params [{} {} {}]}))
  (is (= (sut/process-operator {} [:case [{} {} {} {}]])
        {:raw-string "CASE (?) WHEN (?) THEN (?) ELSE (?) END", :params [{} {} {} {}]}))
  (is (= (sut/process-operator {} [:case [{} {} {} {} {}]])
        {:raw-string "CASE (?) WHEN (?) THEN (?) WHEN (?) THEN (?) END", :params [{} {} {} {} {}]}))
  (is (= (sut/process-operator {} [:case [{} {} {} {} {} {}]])
        {:raw-string "CASE (?) WHEN (?) THEN (?) WHEN (?) THEN (?) ELSE (?) END", :params [{} {} {} {} {} {}]})))

(deftest cond-test
  (is (= (sut/process-operator {} [:cond [{} {}]])
        {:raw-string "CASE WHEN (?) THEN (?) END", :params [{} {}]}))
  (is (= (sut/process-operator {} [:cond [{} {} {} {}]])
        {:raw-string "CASE WHEN (?) THEN (?) WHEN (?) THEN (?) END", :params [{} {} {} {}]}))
  (is (= (sut/process-operator {} [:cond [{} {} {} {} {} {}]])
        {:raw-string "CASE WHEN (?) THEN (?) WHEN (?) THEN (?) WHEN (?) THEN (?) END", :params [{} {} {} {} {} {}]})))

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

#?(:clj
   (deftest operator-sql-names-test
     (is (= (sut/operator-sql-names {:upper-case? true} 'abc-def-ghi)
           [:abc-def-ghi "ABC_DEF_GHI"]))))
