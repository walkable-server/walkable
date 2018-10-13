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

(def kid-toy-schema
  {:columns          [:kid/name :toy/index :toy/color]
   :idents           {:kid/by-id :kid/number
                      :kids/all  "kid"}
   :extra-conditions {}
   :joins            {:toy/owner [:toy/owner-number :kid/number]}
   :reversed-joins   {:kid/toy :toy/owner}
   :cardinality      {:kid/by-id :one
                      :kid/toy   :one
                      :toy/owner :one}})

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
      {:farmers/all [#:farmer{:number 2, :name "mary", :cow #:cow {:index 20, :color "brown"}}]}}]}

   :kid-toy
   {:core-schema kid-toy-schema
    :test-suite
    [{:message "idents should work"
      :query
      '[{[:kid/by-id 1] [:kid/number :kid/name
                         {:kid/toy [:toy/index :toy/color]}]}]
      :expected
      {[:kid/by-id 1] #:kid {:number 1, :name "jon", :toy #:toy {:index 10, :color "yellow"}}}}]}})
