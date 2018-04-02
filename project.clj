(defproject walkable "1.0.0-beta2"
  :description "A serious way to fetch data from SQL using Clojure: Datomic pull syntax, Clojure flavored filtering and more."
  :url "https://github.com/walkable-server/walkable"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clojure-future-spec "1.9.0-beta4"]
                 [com.wsscode/pathom "2.0.0-beta1"]]
  :resource-paths ["resources"]
  :profiles
  {:dev          [:project/dev :profiles/dev]
   :repl         {:prep-tasks   ^:replace ["javac" "compile"]
                  :repl-options {:init-ns          user
                                 :timeout          120000
                                 :nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}}
   :uberjar      {:aot :all}
   :profiles/dev {}
   :project/dev  {:main           ^:skip-aot walkable-demo.main
                  ;; :prep-tasks     ["javac" "compile" ["run" ":duct/compiler"]]
                  :plugins        [[duct/lein-duct "0.10.6"]]
                  :source-paths   ["dev/src"]
                  :resource-paths ["dev/resources" "target/resources"]
                  :dependencies   [[duct/core "0.6.2"]
                                   [duct/module.logging "0.3.1"]
                                   [duct/logger.timbre "0.4.1"]
                                   [duct/module.web "0.6.4"]
                                   [duct/module.ataraxy "0.2.0"]

                                   [duct/module.sql "0.4.2"]
                                   [duct/database.sql.hikaricp "0.3.2"]

                                   ;; enable if you use postgresql
                                   ;; [org.postgresql/postgresql "42.2.1"]
                                   ;; enable if you use mysql
                                   ;; [mysql/mysql-connector-java "5.1.45"]
                                   [org.xerial/sqlite-jdbc "3.21.0.1"]

                                   [org.clojure/clojurescript "1.9.946"]
                                   [duct/module.cljs "0.3.2" :exclusions [org.clojure/clojurescript]]
                                   [duct/server.figwheel "0.2.1" :exclusions [org.clojure/clojurescript]]
                                   [devcards "0.2.4" :exclusions [org.clojure/clojurescript]]

                                   [fulcrologic/fulcro "2.3.0" :exclusions [org.clojure/clojurescript]]
                                   [fulcrologic/fulcro-spec "2.0.3-1" :scope "test" :exclusions [fulcrologic/fulcro]]

                                   [integrant/repl "0.3.0"]
                                   [eftest "0.4.3"]
                                   [kerodon "0.9.0"]]}})
