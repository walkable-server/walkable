(defproject walkable "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/clojurescript "1.9.946"]
                 [clojure-future-spec "1.9.0-beta4"]
                 [duct/core "0.6.2"]
                 [duct/module.logging "0.3.1"]
                 [duct/logger.timbre "0.4.1"]
                 [duct/module.web "0.6.4"]
                 [duct/module.ataraxy "0.2.0"]
                 [duct/module.cljs "0.3.1" :exclusions [org.clojure/clojurescript]]
                 [duct/server.figwheel "0.2.1" :exclusions [org.clojure/clojurescript]]
                 [devcards "0.2.1" :exclusions [org.clojure/clojurescript]]
                 [duct/module.sql "0.4.2"]
                 [com.wsscode/pathom "2.0.0-beta1"]
                 [fulcrologic/fulcro "2.2.0" :exclusions [org.clojure/clojurescript]]
                 [fulcrologic/fulcro-spec "2.0.1" :scope "test" :exclusions [fulcrologic/fulcro]]]
  :plugins [[duct/lein-duct "0.10.6"]]
  :main ^:skip-aot foo.main
  :resource-paths ["resources" "target/resources"]
  :prep-tasks     ["javac" "compile" ["run" ":duct/compiler"]]
  :profiles
  {:dev  [:project/dev :profiles/dev]
   :repl {:prep-tasks   ^:replace ["javac" "compile"]
          :repl-options {:init-ns user
                         :timeout 120000
                         :nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}}
   :uberjar {:aot :all}
   :profiles/dev {}
   :project/dev  {:source-paths   ["dev/src"]
                  :resource-paths ["dev/resources"]
                  :dependencies   [[integrant/repl "0.2.0"]
                                   [eftest "0.4.0"]
                                   [org.postgresql/postgresql "42.1.4"]
                                   [duct/database.sql.hikaricp "0.3.2"]
                                   [kerodon "0.9.0"]]}})
