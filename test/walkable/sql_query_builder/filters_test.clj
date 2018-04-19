(ns walkable.sql-query-builder.filters-test
  (:require [walkable.sql-query-builder.filters :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest inline-params-tests
  (is (= (sut/inline-params
           {:raw-string " ? "
            :params     [{:raw-string "2018 - `human`.`yob`"
                          :params     []}]})
        {:raw-string " 2018 - `human`.`yob` "
         :params     []})))

(deftest and-tests
  (is (= (sut/process-operator {} [:and []])
        {:raw-string "(?)",
         :params [{:raw-string " ? ", :params [true]}]}))
  (is (= (sut/process-operator {} [:and [{}]])
        {:raw-string "(?)", :params [{}]}))
  (is (= (sut/process-operator {} [:and [{} {}]])
        {:raw-string "(?) AND (?)", :params [{} {}]}))
  (is (= (sut/process-operator {} [:and [{} {} {}]])
        {:raw-string "(?) AND (?) AND (?)", :params [{} {} {}]})))

(deftest or-tests
  (is (= (sut/process-operator {} [:or []])
        {:raw-string "NULL", :params []}))
  (is (= (sut/process-operator {} [:or [{}]])
        {:raw-string "(?)", :params [{}]}))
  (is (= (sut/process-operator {} [:or [{} {}]])
        {:raw-string "(?) OR (?)", :params [{} {}]}))
  (is (= (sut/process-operator {} [:or [{} {} {}]])
        {:raw-string "(?) OR (?) OR (?)", :params [{} {} {}]})))

(deftest in-tests
  (is (= (sut/process-operator {} [:in []])
        {:raw-string "(?) IN ()", :params []}))
  (is (= (sut/process-operator {} [:in [{}]])
        {:raw-string "(?) IN ()", :params [{}]}))
  (is (= (sut/process-operator {} [:in [{} {}]])
        {:raw-string "(?) IN (?)", :params [{} {}]}))
  (is (= (sut/process-operator {} [:in [{} {} {}]])
        {:raw-string "(?) IN (?, ?)", :params [{} {} {}]})))

(deftest not-tests
  (is (= (sut/process-operator {} [:not [{}]])
        {:raw-string "NOT (?)", :params [{}]})))

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

(deftest parameterize-tests
  (is (= (sut/parameterize {:column-names {:a/foo "a.foo"
                                           :b/bar "b.bar"}
                            :join-filter-subqueries
                            {:x/a "x.a_id IN (SELECT a.id FROM a WHERE ?)"
                             :x/b "x.id IN (SELECT x_b.x_id FROM x_b JOIN b ON b.id = x_b.b_id WHERE ?)"}}
           [:or {:x/a [:= :a/foo "meh"]}
                {:x/b [:= :b/bar "mere"]}])
        {:params ["meh" "mere"],
         :raw-string (str "((x.a_id IN (SELECT a.id FROM a"
                       " WHERE (a.foo)=( ? ))))"
                       " OR ((x.id IN (SELECT x_b.x_id FROM x_b JOIN b ON b.id = x_b.b_id"
                       " WHERE (b.bar)=( ? ))))")})))
