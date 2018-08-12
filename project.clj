(defproject walkable "1.0.0-SNAPSHOT"
  :description "A serious way to fetch data from SQL using Clojure: Datomic pull syntax, Clojure flavored filtering and more."
  :url "https://github.com/walkable-server/walkable"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.9.0" :scope "provided"]
                 [org.clojure/clojurescript "1.10.339" :scope "provided"]
                 [clojure-future-spec "1.9.0-beta4"]
                 ;; not work with latest spec version yet
                 ;;[org.clojure/spec.alpha "0.2.168"]
                 [com.wsscode/pathom "2.0.19"]
                 [org.clojure/core.async "0.4.474" :scope "provided"]]
  :resource-paths ["resources"]
  :profiles
  {:dev          [:project/dev :profiles/dev]
   :repl         {:prep-tasks   ^:replace ["javac" "compile"]
                  :repl-options {:init-ns          user
                                 :timeout          120000}}
   :profiles/dev {}
   :project/dev  {:main           ^:skip-aot walkable-demo.main
                  ;; :prep-tasks     ["javac" "compile" ["run" ":duct/compiler"]]
                  :plugins        [[duct/lein-duct "0.10.6"]]
                  :source-paths   ["dev/src" "dev/test"]
                  :resource-paths ["dev/resources" "target/resources"]
                  :dependencies   [[duct/core "0.6.2"]
                                   [duct/module.logging "0.3.1"]
                                   [duct/logger.timbre "0.4.1"]
                                   [org.clojure/test.check "0.10.0-alpha3"]
                                   ;;[duct/module.web "0.6.4"]
                                   ;;[duct/module.ataraxy "0.2.0"]
                                   [cheshire "5.8.0"]

                                   [duct/module.sql "0.4.2"]
                                   [duct/database.sql.hikaricp "0.3.3"]

                                   ;; enable if you use postgresql
                                   ;; [org.postgresql/postgresql "42.2.4"]
                                   ;; enable if you use mysql
                                   ;; [mysql/mysql-connector-java "5.1.45"]
                                   [org.xerial/sqlite-jdbc "3.23.1"]

                                   [integrant/repl "0.3.1"]
                                   [eftest "0.5.2"]
                                   [kerodon "0.9.0"]]}})
