(ns walkable.core
  (:require [clojure.zip :as z]
            [walkable.sql-query-builder.expressions :as expressions]
            [walkable.sql-query-builder.ast :as ast]
            [walkable.sql-query-builder.floor-plan :as floor-plan]
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

(defn ->build-and-run-query
  [query-env]
  (fn [env entities prepared-query]
    (let [q (-> (prepared-query env entities)
              (expressions/build-parameterized-sql-query))]
      (if q
        (query-env env q)
        []))))

;; top-down process
(defn fetch-data
  [build-and-run-query env ast]
  (->> ast
       (ast-map-loc (fn [loc]
                      (let [{:keys [::ast/prepared-query]} (z/node loc)]
                        (if prepared-query
                          (let [parent (last (z/path loc))
                                ;; TODO: partition entities and concat back
                                entities (build-and-run-query env (:entities parent) prepared-query)]
                            (z/edit loc #(-> % (dissoc ::ast/prepared-query) (assoc :entities entities))))
                          loc))))))

;; bottom-up process, start from lowest levels (ones without children)
;; and go up using z/up and prepared-merge-sub-entities

(defn move-to-nth-child
  [loc n]
  (nth (iterate z/right (z/down loc)) n))

(defn merge-data-in-bottom-branches*
  [wrap-merge ast]
  (loop [loc (ast/ast-zipper ast)]
    (if (z/end? loc)
      (z/root loc)
      (recur
       (z/next
        (let [{:keys [:children] :as node} (z/node loc)
              {:keys [::ast/prepared-merge-sub-entities]} node]
          (if (or (not prepared-merge-sub-entities) ;; node can be nil
                  (not-empty children))
            loc
            (let [parent (z/up loc)
                  ;; save current position
                  position-to-parent (count (z/lefts loc))

                  merged-entities
                  ((wrap-merge prepared-merge-sub-entities)
                   (:entities (z/node parent))
                   (:entities node))]
              (-> (z/edit parent assoc :entities merged-entities)
                  ;; come back to previous position
                  (move-to-nth-child position-to-parent))))))))))

(defn merge-data-in-bottom-branches
  [wrap-merge ast]
  (->> (merge-data-in-bottom-branches* wrap-merge ast)
       (ast/filterz #(not-empty (:children %)))))

(defn merge-data
  [wrap-merge ast]
  (loop [{:keys [:children] :as root} ast]
    (if (empty? children)
      (:entities root)
      (recur (merge-data-in-bottom-branches wrap-merge root)))))

(defn ast-resolver*
  [{:keys [:build-and-run-query :floor-plan :env :wrap-merge :ast]}]
  (->> (ast/prepare-ast floor-plan ast)
       (fetch-data build-and-run-query env)
       (merge-data wrap-merge)))

(defn prepared-ast-resolver*
  [{:keys [:build-and-run-query :env :wrap-merge :prepared-ast]}]
  (->> prepared-ast
       (fetch-data build-and-run-query env)
       (merge-data wrap-merge)))

(defn query-resolver*
  [{:keys [:floor-plan :env :resolver :query]}]
  (resolver floor-plan env (p/query->ast query)))

(defn ast-resolver
  [floor-plan query-env env ast]
  (ast-resolver* {:floor-plan floor-plan
                  :build-and-run-query (->build-and-run-query query-env)
                  :env env
                  :wrap-merge identity
                  :ast ast}))

(defn prepared-ast-resolver
  [query-env env prepared-ast]
  (prepared-ast-resolver* {:env env
                           :build-and-run-query (->build-and-run-query query-env)
                           :wrap-merge identity
                           :prepared-ast prepared-ast}))

(defn query-resolver
  [floor-plan query-env env query]
  (query-resolver* {:floor-plan floor-plan
                    :build-and-run-query (->build-and-run-query query-env)
                    :env env
                    :resolver ast-resolver
                    :query query}))

(defn ident-keyword [env]
  (-> env ::pcp/node ::pcp/input ffirst))

(defn ident
  [env]
  (when-let [k (ident-keyword env)]
    [k (get (p/entity env) k)]))

(defn wrap-with-ident
  [ast ident]
  (if ident
    (let [main (assoc ast :type :join :key ident :dispatch-key (first ident))]
      {:type :root
       :children [main]})
    ast))

(comment
  (p/ast->query (wrap-with-ident (p/query->ast [:x/a :x/b {:x/c [:c/d]}]) [:x/i 1]))
  [{[:x/i 1] [:x/a :x/b #:x{:c [:c/d]}]}])

(defn dynamic-resolver
  [floor-plan query-env env]
  (let [i (ident env)
        ast (-> env ::pcp/node ::pcp/foreign-ast
                (wrap-with-ident i))
        result (ast-resolver floor-plan query-env env ast)]
    (if i
      (get result i)
      result)))

(defn compute-indexes [resolver-sym ios]
  (reduce (fn [acc x] (pc/add acc resolver-sym x))
          {}
          ios))

(defn internalize-indexes
  [indexes {:keys [::pc/sym] :as dynamic-resolver}]
  (-> indexes
    (update ::pc/index-resolvers
      (fn [resolvers]
        (into {}
          (map (fn [[r v]] [r (assoc v ::pc/dynamic-sym sym)]))
          resolvers)))
    (assoc-in [::pc/index-resolvers sym]
      dynamic-resolver)))

(defn connect-plugin
  [{:keys [:resolver-sym :registry :resolver :autocomplete-ignore :db-type :query-env]
    :or   {resolver dynamic-resolver
           ;; query-env (->query-env :db)
           resolver-sym `walkable-resolver}}]
  (let [{:keys [:inputs-outputs] compiled-floor-plan :floor-plan}
        (floor-plan/compile-floor-plan (if db-type
                                         (floor-plan/with-db-type db-type registry)
                                         registry))]
    {::p/wrap-parser2
     (fn [parser {:keys [::p/plugins]}]
       (let [resolve-fn  (fn [env _]
                           (resolver compiled-floor-plan query-env env))
             all-indexes (-> (compute-indexes resolver-sym inputs-outputs)
                           (internalize-indexes
                             {::pc/sym               (gensym resolver-sym)
                              ::pc/cache?            false
                              ::pc/dynamic-resolver? true
                              ::pc/resolve           resolve-fn})
                           (merge {::pc/autocomplete-ignore (or autocomplete-ignore #{})}))
             idx-atoms   (keep ::pc/indexes plugins)]
         (doseq [idx* idx-atoms]
           (swap! idx* pc/merge-indexes all-indexes))
         (fn [env tx] (parser env tx))))}))
