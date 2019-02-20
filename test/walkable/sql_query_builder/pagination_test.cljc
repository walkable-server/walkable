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

(deftest offset-fallback-test
  (is (= (mapv (sut/offset-fallback emitter/default-emitter
                 {:default 2 :validate #(<= 2 % 4)})
           (range 8))
        (mapv #(str " OFFSET " %) [2 2 2 3 4 2 2 2])))
  (is (= (map (sut/offset-fallback emitter/default-emitter
                {:default 2 :validate #(<= 2 % 4)})
           [:invalid 'types])
        (mapv #(str " OFFSET " %) [2 2]))))

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
                            {:people/all
                             {:offset   {:default  5
                                         :validate #(<= 2 % 4)}
                              :limit    {:default  10
                                         :validate #(<= 12 % 14)}
                              :order-by {:default  [:x/a]
                                         :validate #{:x/a :x/b}}}})
        current-fallbacks (:people/all all-fallbacks)
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
