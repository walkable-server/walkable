(defproject walkable "1.2.0-SNAPSHOT"
  :description "A Clojure(script) SQL library for building APIs"
  :url "https://walkable.gitlab.io"
  :license {:name         "Eclipse Public License - v 1.0"
            :url          "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments     "same as Clojure"}
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
   :project/common    {:plugins      [[duct/lein-duct "0.12.1"]]
                       :dependencies [[duct/core "0.7.0"]
                                      [duct/module.logging "0.4.0"]
                                      [duct/module.sql "0.5.0"]
                                      [org.clojure/test.check "0.10.0-alpha3"]
                                      [cheshire "5.8.0"]

                                      [integrant/repl "0.3.1"]
                                      [eftest "0.5.4"]
                                      [kerodon "0.9.0"]]}})
