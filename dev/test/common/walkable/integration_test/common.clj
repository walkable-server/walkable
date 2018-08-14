(ns walkable.integration-test.common)

(def farmer-cow-schema
  {:columns          [:cow/color
                      :farmer/number
                      :farmer/name]
   :idents           {:farmer/by-id :farmer/number
                      :farmers/all  "farmer"}
   :extra-conditions {}
   :joins            {:farmer/cow [:farmer/cow-index :cow/index]}
   :reversed-joins   {:cow/owner :farmer/cow}
   :cardinality      {:farmer/by-id :one
                      :cow/owner    :one
                      :farmer/cow   :one}})

(def common-scenarios
  {:farmer-cow
   {:core-schema farmer-cow-schema
    :test-suite
    [{:message "filters should work"
      :query
      `[{(:farmers/all {:filters {:farmer/cow [{:cow/owner [:= :farmer/name "mary"]}]}})
         [:farmer/number :farmer/name
          {:farmer/cow [:cow/index :cow/color]}]}]
      :expected
      {:farmers/all [#:farmer{:number 2, :name "mary", :cow #:cow {:index 20, :color "brown"}}]}}]}})
