(ns walkable.sql-query-builder.pagination-test
  (:require [walkable.sql-query-builder.pagination :as sut]
            [walkable.sql-query-builder.emitter :as emitter]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest testing is are]]))

(deftest ->stringify-order-by-tests
  (is (= ((sut/->stringify-order-by
            {:asc        " ASC"
             :desc       " DESC"
             :nils-first " NULLS FIRST"
             :nils-last  " NULLS LAST"})
          {:person/name "`p`.`n`" :person/age "`p`.`a`"}
          [{:column :person/name} {:column :person/age, :params [:desc :nils-last]}])
        " ORDER BY `p`.`n`, `p`.`a` DESC NULLS LAST"))

  (is (nil? ((sut/->stringify-order-by
               {:asc        " ASC"
                :desc       " DESC"
                :nils-first " NULLS FIRST"
                :nils-last  " NULLS LAST"})
             {:person/name "`p`.`n`" :person/age "`p`.`a`"}
             nil))))

(deftest ->conform-order-by-test
  (are [order-by conformed]
      (= ((sut/->conform-order-by #{:asc :desc :nils-first :nils-last})
          order-by)
        conformed)

    :x/a
    [{:column :x/a}]

    [:x/a :asc :x/b :desc :nils-first :x/c]
    [{:column :x/a, :params [:asc]}
     {:column :x/b, :params [:desc :nils-first]}
     {:column :x/c}]

    [:x/a :asc :x/b :desc :nils-first 'invalid-type]
    ::s/invalid

    [:x/a :asc :x/b :desc :nils-first]
    [{:column :x/a, :params [:asc]}
     {:column :x/b, :params [:desc :nils-first]}]

    :invalid-type
    ::s/invalid))

(deftest wrap-validate-order-by-test
  (let [simple-validate  (sut/wrap-validate-order-by #{:x/a :x/b})
        default-validate (sut/wrap-validate-order-by nil)]
    (are [validate conformed-order-by valid?]
        (= (validate conformed-order-by) valid?)

      simple-validate
      [{:column :x/a, :params [:asc]} {:column :x/b, :params [:desc :nils-first]}]
      true

      simple-validate
      [{:column :x/a, :params [:asc]}
       {:column :x/invalid-key, :params [:desc :nils-first]}]
      false

      default-validate
      [{:column :x/a, :params [:asc]}
       {:column :x/b, :params [:desc :nils-first]}]
      true

      default-validate
      [{:column :x/a, :params [:asc]}
       {:column :x/any-key, :params [:desc :nils-first]}]
      true)))

(deftest offset-fallback-with-default-emitter-test
  (is (= (mapv (sut/offset-fallback emitter/default-emitter
                 {:default 99 :validate #(<= 2 % 4)})
           (range 8))
        (mapv #(str " OFFSET " %) [99 99 2 3 4 99 99 99])))
  (is (= (map (sut/offset-fallback emitter/default-emitter
                {:default 99 :validate #(<= 2 % 4)})
           [:invalid 'types])
        (mapv #(str " OFFSET " %) [99 99]))))

(deftest limit-fallback-with-default-emitter-test
  (is (= (mapv (sut/limit-fallback emitter/default-emitter
                 {:default 99 :validate #(<= 2 % 4)})
           (range 8))
        (mapv #(str " LIMIT " %) [99 99 2 3 4 99 99 99])))
  (is (= (map (sut/limit-fallback emitter/default-emitter
                {:default 99 :validate #(<= 2 % 4)})
           [:invalid 'types])
        (mapv #(str " LIMIT " %) [99 99])))
  (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
        #"Malformed"
        ((sut/limit-fallback emitter/default-emitter
           {:default 99 :validate #(<= 2 % 4)
            :throw?  true})
         :abc)))
  (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
        #"Invalid"
        ((sut/limit-fallback emitter/default-emitter
           {:default 99 :validate #(<= 2 % 4)
            :throw?  true})
         1))))

(defn oracle-validate-limit
  [{:keys [limit percent with-ties]}]
  (if percent
    (< 0 limit 5)
    (<= 2 limit 4)))

(deftest limit-fallback-with-oracle-emitter-test
  (is (= (mapv (sut/limit-fallback emitter/oracle-emitter
                 {:default 99 :validate oracle-validate-limit})
           (range 8))
        (mapv #(str " FETCH FIRST " % " ROWS ONLY") [99 99 2 3 4 99 99 99])))
  (is (= (mapv (sut/limit-fallback emitter/oracle-emitter
                 {:default [99 :percent] :validate oracle-validate-limit})
           (mapv #(do [% :percent]) (range 8)))
        [" FETCH FIRST 99 PERCENT ROWS ONLY"
         " FETCH FIRST 1 PERCENT ROWS ONLY"
         " FETCH FIRST 2 PERCENT ROWS ONLY"
         " FETCH FIRST 3 PERCENT ROWS ONLY"
         " FETCH FIRST 4 PERCENT ROWS ONLY"
         " FETCH FIRST 99 PERCENT ROWS ONLY"
         " FETCH FIRST 99 PERCENT ROWS ONLY"
         " FETCH FIRST 99 PERCENT ROWS ONLY"]))
  (is (= (mapv (sut/limit-fallback emitter/oracle-emitter
                 {:default [99 :percent] :validate oracle-validate-limit})
           (mapv #(do [% :percent :with-ties]) (range 8)))
        [" FETCH FIRST 99 PERCENT ROWS ONLY"
         " FETCH FIRST 1 PERCENT ROWS WITH TIES"
         " FETCH FIRST 2 PERCENT ROWS WITH TIES"
         " FETCH FIRST 3 PERCENT ROWS WITH TIES"
         " FETCH FIRST 4 PERCENT ROWS WITH TIES"
         " FETCH FIRST 99 PERCENT ROWS ONLY"
         " FETCH FIRST 99 PERCENT ROWS ONLY"
         " FETCH FIRST 99 PERCENT ROWS ONLY"]))
  (is (= (mapv (sut/limit-fallback emitter/oracle-emitter
                 {:default [99 :percent]})
           (mapv #(do [% :percent :with-ties]) (range 8)))
        [" FETCH FIRST 0 PERCENT ROWS WITH TIES"
         " FETCH FIRST 1 PERCENT ROWS WITH TIES"
         " FETCH FIRST 2 PERCENT ROWS WITH TIES"
         " FETCH FIRST 3 PERCENT ROWS WITH TIES"
         " FETCH FIRST 4 PERCENT ROWS WITH TIES"
         " FETCH FIRST 5 PERCENT ROWS WITH TIES"
         " FETCH FIRST 6 PERCENT ROWS WITH TIES"
         " FETCH FIRST 7 PERCENT ROWS WITH TIES"]))
  (is (= (mapv (sut/limit-fallback emitter/oracle-emitter
                 {:default [99 :percent]})
           (mapv #(do [% :percent :with-ties-typo]) (range 8)))
        (repeat 8 " FETCH FIRST 99 PERCENT ROWS ONLY")))
  (is (= (map (sut/limit-fallback emitter/oracle-emitter
                {:default 99 :validate #(<= 2 % 4)})
           [:invalid 'types])
        (mapv #(str " FETCH FIRST " % " ROWS ONLY") [99 99])))
  (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
        #"Malformed"
        ((sut/limit-fallback emitter/oracle-emitter
           {:default 99 :validate oracle-validate-limit
            :throw?  true})
         :abc)))
  (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
        #"Invalid"
        ((sut/limit-fallback emitter/oracle-emitter
           {:default 99 :validate oracle-validate-limit
            :throw?  true})
         1))))

(deftest order-by-fallback-test
  (is (= (mapv (sut/order-by-fallback
                 emitter/default-emitter
                 {:x/a "x.a" :x/b "x.b"}
                 {:default  [:x/a :asc :x/b]
                  :validate #{:x/a :x/b}})
           [[:x/a :desc :x/b :desc :nils-first]
            [:x/a :desc :x/invalid-key :desc :nils-first]
            nil])
        [{:columns #{:x/a :x/b},
          :string  " ORDER BY x.a DESC, x.b DESC NULLS FIRST"}
         {:columns #{:x/a :x/b},
          :string  " ORDER BY x.a ASC, x.b"}
         {:columns #{:x/a :x/b},
          :string  " ORDER BY x.a ASC, x.b"}])))

(deftest merge-pagination-test
  (let [all-fallbacks     (sut/compile-fallbacks
                            emitter/default-emitter
                            {:x/a "`x/a`" :x/b "`x/b`" :x/random-key "`x/random-key`"}
                            {:people/people
                             {:offset   {:default  5
                                         :validate #(<= 2 % 4)}
                              :limit    {:default  10
                                         :validate #(<= 12 % 14)}
                              :order-by {:default  [:x/a]
                                         :validate #{:x/a :x/b}}}})
        current-fallbacks (:people/people all-fallbacks)
        default-fallbacks (get all-fallbacks `sut/default-fallbacks)]
    (is (= (sut/merge-pagination
             default-fallbacks
             nil
             {:offset   4
              :limit    8
              :order-by [{:column :x/b}]})
          {:offset           " OFFSET 4",
           :limit            " LIMIT 8",
           :order-by         nil,
           :order-by-columns nil}))
    (is (= (sut/merge-pagination
             default-fallbacks
             current-fallbacks
             {:offset             4
              :limit              8
              :conformed-order-by [:x/invalid-key]})
          {:offset           " OFFSET 4",
           :limit            " LIMIT 10",
           :order-by         " ORDER BY `x/a`",
           :order-by-columns #{:x/a}}))
    (is (= (sut/merge-pagination
             default-fallbacks
             current-fallbacks
             {:offset   4
              :limit    :invalid-type
              :order-by [:x/a :x/b]})
          {:offset           " OFFSET 4",
           :limit            " LIMIT 10",
           :order-by         " ORDER BY `x/a`, `x/b`",
           :order-by-columns #{:x/a :x/b}}))))
