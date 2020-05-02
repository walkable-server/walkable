(ns walkable.core
  (:require
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.emitter :as emitter]
            [walkable.sql-query-builder.ast :as ast]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [walkable.sql-query-builder.pathom-env :as env]
            [clojure.spec.alpha :as s]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.connect.planner :as pcp]))

(defn dynamic-resolver
  [floor-plan env])

(defn compute-indexes [resolver-sym ios]
  (reduce (fn [acc x] (pc/add acc resolver-sym x))
    {}
    ios))

(defn internalize-indexes
  [indexes {::pc/keys [sym] :as dynamic-resolver}]
  (-> indexes
    (update ::pc/index-resolvers
      (fn [resolvers]
        (into {}
          (map (fn [[r v]] [r (assoc v ::pc/dynamic-sym sym)]))
          resolvers)))
    (assoc-in [::pc/index-resolvers sym]
      dynamic-resolver)))

(defn connect-plugin
  [{:keys [resolver-sym db query floor-plan
           inputs-outputs autocomplete-ignore
           resolver]
    :or   {resolver     dynamic-resolver
           resolver-sym `walkable-resolver}}]
  (let [provided-indexes    (compute-indexes resolver-sym inputs-outputs)
        compiled-floor-plan (floor-plan/compile-floor-plan
                             (assoc floor-plan :idents (::pc/idents provided-indexes)))
        config              {::db           db
                             ::query        query
                             ::resolver-sym resolver-sym
                             ::floor-plan   compiled-floor-plan}]
    {::p/intercept-output (fn [_env v] v)
     ::p/wrap-parser2
     (fn [parser {::p/keys [plugins]}]
       (let [resolve-fn  (fn [env _] (resolver compiled-floor-plan env))
             all-indexes (-> provided-indexes
                             (internalize-indexes
                              {::config               config
                               ::pc/sym               (gensym resolver-sym)
                               ::pc/cache?            false
                               ::pc/dynamic-resolver? true
                               ::pc/resolve           resolve-fn})
                             (merge {::pc/autocomplete-ignore (or autocomplete-ignore #{})}))
             idx-atoms   (keep ::pc/indexes plugins)]
         (doseq [idx* idx-atoms]
           (swap! idx* pc/merge-indexes all-indexes))
         (fn [env tx] (parser env tx))))}))
