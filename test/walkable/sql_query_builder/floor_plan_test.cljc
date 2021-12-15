(ns walkable.sql-query-builder.floor-plan-test
  (:require [walkable.sql-query-builder.floor-plan :as sut]
            [walkable.sql-query-builder.emitter :as emitter]
            [clojure.test :as t :refer [deftest testing is]]))

(deftest keyword-prop-tests
  (is (= (sut/keyword-prop {:emitter emitter/default-emitter :attributes []}
           :a/b :table-name)
        "\"a\""))
  (is (= (sut/keyword-prop {:emitter emitter/default-emitter :attributes [{:key :a/b
                                                                           :type :true-column
                                                                           :table-name "z"}]}
           :a/b :table-name)
        "z")))

