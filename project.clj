(defproject walkable "1.2.0-snapshot"
  :description "A serious way to fetch data from SQL using Clojure: Datomic pull syntax, Clojure flavored filtering and more."
  :url "https://github.com/walkable-server/walkable"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.9.0" :scope "provided"]
                 [org.clojure/clojurescript "1.10.339" :scope "provided"]
                 [org.clojure/spec.alpha "0.2.176"]
                 [com.wsscode/pathom "2.2.0"]
                 [prismatic/plumbing "0.5.5"]
                 [org.clojure/core.async "0.4.474" :scope "provided"]]
  :resource-paths ["resources"]
  :test-selectors {:default     (complement :integration)
                   :integration :integration}
  :profiles
  {:dev      [:project/common]
   :sqlite   [:project/common :profiles/sqlite]
   :mysql    [:project/common :profiles/mysql]
   :postgres [:project/common :profiles/postgres]
   :repl     {:prep-tasks   ^:replace ["javac" "compile"]
              :repl-options {:init-ns user
                             :timeout 120000}}

   :profiles/sqlite   {:source-paths   ["dev/src/common" "dev/src/sqlite"]
                       :test-paths     ["dev/test/common" "dev/test/sqlite"]
                       :resource-paths ["dev/resources/common" "dev/resources/sqlite"]
                       :dependencies   [[org.xerial/sqlite-jdbc "3.23.1"]]}
   :profiles/mysql    {:source-paths   ["dev/src/common" "dev/src/mysql"]
                       :test-paths     ["dev/test/common" "dev/test/mysql"]
                       :resource-paths ["dev/resources/common" "dev/resources/mysql"]
                       :dependencies   [[mysql/mysql-connector-java "8.0.12"]]}
   :profiles/postgres {:source-paths   ["dev/src/common" "dev/src/postgres"]
                       :test-paths     ["dev/test/postgres" "dev/test/common"]
                       :resource-paths ["dev/resources/common" "dev/resources/postgres"]
                       :dependencies   [[org.postgresql/postgresql "42.2.4"]]}
   :project/common    {:plugins      [[duct/lein-duct "0.10.6"]]
                       :dependencies [[duct/core "0.6.2"]
                                      [duct/module.logging "0.3.1"]
                                      [duct/logger.timbre "0.4.1"]
                                      [org.clojure/test.check "0.10.0-alpha3"]
                                      [cheshire "5.8.0"]

                                      [duct/module.sql "0.4.2"]
                                      [duct/database.sql.hikaricp "0.3.3"]

                                      [integrant/repl "0.3.1"]
                                      [eftest "0.5.2"]
                                      [kerodon "0.9.0"]]}})
