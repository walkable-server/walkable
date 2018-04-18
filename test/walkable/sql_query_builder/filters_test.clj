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
        {:raw-string "(?)=(?)", :params ({} {})}))
  (is (= (sut/process-operator {} [:= [{} {} {}]])
        {:raw-string "(?)=(?) AND (?)=(?)", :params ({} {} {} {})}))
  (is (= (sut/process-operator {} [:= [{} {} {} {}]])
        {:raw-string "(?)=(?) AND (?)=(?) AND (?)=(?)", :params ({} {} {} {} {} {})})))

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
