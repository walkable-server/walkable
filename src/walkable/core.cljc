(ns walkable.core
  (:require [clojure.zip :as z]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.ast :as ast]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
            [clojure.spec.alpha :as s]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.connect.planner :as pcp]))

(defn ast-map-loc [f ast]
  (loop [loc (ast/ast-zipper ast)]
    (if (z/end? loc)
      (z/root loc)
      (recur
       (z/next
        (let [node (z/node loc)]
          (if (= :root (:type node))
            loc
            (f loc))))))))

;; top-down process
(defn fetch-data
  [env ast]
  (->> ast
       (ast-map-loc (fn [loc]
                      (let [{::ast/keys [prepared-query]} (z/node loc)]
                        (if prepared-query
                          (let [parent (last (z/path loc))
                                ;; TODO: partition entities and concat back
                                q (->> (:entities parent)
                                       (prepared-query env)
                                       (expressions/build-parameterized-sql-query))
                                entities ((::run env) (::db env) q)]
                            (z/edit loc #(-> % (dissoc ::ast/prepared-query) (assoc :entities entities))))
                          loc))))))

;; bottom-up process, start from lowest levels (ones without children)
;; and go up using z/up and prepared-merge-sub-entities

(defn move-to-nth-child
  [loc n]
  (nth (iterate z/right (z/down loc)) n))

(defn merge-data-in-bottom-branches*
  [ast]
  (loop [loc (ast/ast-zipper ast)]
    (if (z/end? loc)
      (z/root loc)
      (recur
       (z/next
        (let [{:keys [children] :as node} (z/node loc)
              {::ast/keys [prepared-merge-sub-entities]} node]
          (if (or (not prepared-merge-sub-entities) ;; node can be nil
                  (not-empty children))
            loc
            (let [parent (z/up loc)
                  ;; save current position
                  position-to-parent (count (z/lefts loc))

                  merged-entities
                  (prepared-merge-sub-entities (:entities (z/node parent))
                                               (:entities node))]
              (-> (z/edit parent assoc :entities merged-entities)
                  ;; come back to previous position
                  (move-to-nth-child position-to-parent))))))))))

(defn merge-data-in-bottom-branches
  [ast]
  (->> (merge-data-in-bottom-branches* ast)
       (ast/filterz #(not-empty (:children %)))))

(defn merge-data
  [ast]
  (loop [{:keys [children] :as root} ast]
    (if (empty? children)
      (:entities root)
      (recur (merge-data-in-bottom-branches root)))))

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
