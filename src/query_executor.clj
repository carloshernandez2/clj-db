(ns query-executor
  (:refer-clojure :exclude [and or sort < >])
  (:require
   [clojure.core :as core]
   [clojure.data.csv :as csv]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [heap-file :as heap-file])
  (:import
   (java.io Closeable RandomAccessFile)))

;; Query executor that takes a parsed query plan made up of plan nodes assigned to query keys and executes it retrieving data from csv files.
;; It makes use of lazy sequences as an interface between query plan nodes to avoid fetching all the data when it is not necessary.
;; It supports the following query plan nodes:
;; - Scan: Creates a lazy sequence of maps whose keys correspond to the values of each of the rows provided by a specified csv file
;; - Projection: Selects columns from the intermediate result and returns them
;; - Selection: Filters rows based on max 2 conditions which can be combined with 'AND' or 'OR'
;; - Limit: Limits the number of results
;; - Sort: Sorts in ascending order by the given fields
;; - Nested Loops Join: Cartesian product of two tables to which a filter is applied (JOIN in SQL)
;; - Hash Join: Builds a hash map from one table and uses it to find matching rows in the other. Only applicable for equijoins.
;; - Sort-merge Join: Assumes sorted inputs and creates a 'mini' Cartesian product with rows matching on a value. Only applicable for equijoins.
;; - Aggregate: Groups rows by specified columns and applies aggregate functions (e.g., COUNT, AVG) to the grouped data.

(set! *warn-on-reflection* true)

(defn- file-data->maps [columns heap-file-data]
  (map (partial zipmap columns) heap-file-data))

(defn heap-file-scan [table]
  (fn [{resources :__resources__}]
    (let [table-reader (RandomAccessFile. (str table "_table.cljdb") "r")
          catalog-reader (java.io.PushbackReader. (io/reader (str table "_catalog.edn")))
          catalog (edn/read catalog-reader)
          columns  (into (array-map) (map-indexed (fn [idx val] [val idx])
                                                  (:columns catalog)))]
      {:__result__ [columns (heap-file/scan catalog table-reader)]
       :__resources__ (concat resources [table-reader catalog-reader])})))

(defn csv-scan
  [table]
  (fn [{resources :__resources__}]
    (let [table-reader (io/reader (str table "_table.csv"))
          catalog-reader (java.io.PushbackReader. (io/reader (str table "_catalog.edn")))
          catalog (edn/read catalog-reader)
          columns (into (array-map) (map-indexed (fn [idx val] [val idx])
                                                 (:columns catalog)))]
      {:__result__ [columns (map (partial heap-file/string-row->types catalog)
                                 (rest (csv/read-csv table-reader)))]
       :__resources__ (concat resources [table-reader catalog-reader])})))

(defn projection
  [& cols]
  (fn [{[columns rows] :__result__}]
    (let [indexed-cols (into (array-map) (filter (comp (set cols) first)) columns)]
      {:__result__
       [indexed-cols
        (map (fn [row]
               (into []
                     (keep-indexed #(when ((set/map-invert indexed-cols) %1) %2))
                     row)) rows)]})))

(defn and [res1 res2]
  (core/and res1 res2))

(defn or [res1 res2]
  (core/or res1 res2))

(defn > [n1 n2]
  (pos? (compare n1 n2)))

(defn < [n1 n2]
  (neg? (compare n1 n2)))

(defn selection
  [[fn1 field1 val1] & [expr [fn2 field2 val2]]]
  (fn [{[columns rows] :__result__}]
    {:__result__
     [columns (filter (fn [row]
                        (if fn2
                          (expr (fn1 (get row (columns field1)) val1)
                                (fn2 (get row (columns field2)) val2))
                          (fn1 (get row (columns field1)) val1)))
                      rows)]}))

(defn limit
  [n]
  (fn [{[columns rows] :__result__}]
    {:__result__ [columns (take n rows)]}))

;; NOTE: Breaks sort order ties arbitrarily
(defn sort
  [& fields]
  (fn [{[columns rows] :__result__}]
    (let [keyfn (apply juxt (mapv (fn [field]
                                    #(aget ^objects % (columns field)))
                                  fields))
          queue (java.util.PriorityQueue. #(compare (keyfn %1) (keyfn %2)))
          yield (fn yield []
                  (lazy-seq
                   (when-let [e (.poll queue)]
                     (cons (vec e) (yield)))))
          compress-sort (fn compress-sort [[row & more]]
                          (if row
                            (do (.add queue (into-array Object row))
                                (recur more))
                            (yield)))]
      {:__result__ [columns (lazy-seq (compress-sort rows))]})))

(defn- base-join
  [f t-name]
  (fn [{[columns rows] :__result__ :as iresultset}]
    (let [[t-columns t] (t-name iresultset)
          renamed-t-cols (into (array-map)
                               (map (fn [[k v]]
                                      (if (columns k)
                                        [(keyword (name t-name) (name k)) v] [k v])))
                               t-columns)]
      {:__result__
       [(into (array-map)
              (map (partial vector) (concat (keys columns) (keys renamed-t-cols)) (range)))
        (f [columns rows] [renamed-t-cols t])]})))

(defn nested-loops-join
  [[op v1 v2] t-name]
  (base-join
   (fn [[t1-columns t1-rows] [t2-columns t2-rows]]
     (for [row1 t1-rows row2 t2-rows
           :when (op (get row1 (t1-columns v1))
                     (get row2 (t2-columns v2)))]
       (vec (concat row1 row2)))) t-name))

(defn hash-join
  [[op v1 v2] t-name]
  (assert (= op =) "Hash join only works on equijoins")
  (base-join
   (fn [[t1-columns t1-rows] [t2-columns t2-rows]]
     (let [hash-table (group-by #(get % (t1-columns v1)) t1-rows)]
       (mapcat
        identity
        (for [row2 t2-rows
              :let [field (get row2 (t2-columns v2))]]
          (sequence (map (fn [row1] (vec (concat row1 row2)))) (hash-table field))))))
   t-name))

(defn sort-merge-join
  [[op k1 k2] t-name]
  (assert (= op =) "Sort merge join only works on equijoins")
  (base-join
   (fn [[t1-columns t1-rows] [t2-columns t2-rows]]
     (letfn [(get1 [row] (get row (t1-columns k1)))
             (get2 [row] (get row (t2-columns k2)))
             (drop-rows [s f vmin v] (cond->> s (= vmin v) (drop-while #(= v (f %)))))
             (lazy-join [[r1 & more1 :as s1] [[accr :as accs] [r2 & more2 :as s2]]]
               (let [v1 (when r1 (get1 r1))
                     nv1 (when more1 (get1 (first more1)))
                     v2 (when r2 (get2 r2))]
                 (cond (or (nil? r1) (and (not= v1 nv1) (nil? r2))) []
                       (= v1 v2) (cons (into (vec r1) r2)
                                       (lazy-seq (lazy-join s1 [s2 more2])))
                       (when accr (= nv1 (get2 accr))) (recur (rest s1) [accs accs])
                       :else (let [vmin (if (< v1 v2) v1 v2)
                                   new-s2 (drop-rows s2 get2 vmin v2)]
                               (recur (drop-rows s1 get1 vmin v1) [new-s2 new-s2])))))]
       (lazy-join t1-rows [t2-rows t2-rows]))) t-name))

(defn average [acc v]
  (cond (= acc ::start) [v 1]
        (= v ::end) (/ (first acc) (second acc))
        :else [(+ (first acc) v) (inc (second acc))]))

(defn count' [acc v]
  (cond (= acc ::start) 1
        (= v ::end) acc
        :else (inc acc)))

(defn aggregate [group-cols & ffa]
  (fn [{[columns rows] :__result__}]
    (let [get-fn #(fn [row] (get row (columns %)))
          excluded? (if-let [comparator (when (not-empty group-cols)
                                          (apply juxt (mapv get-fn group-cols)))]
                      (fn [r1 r2]
                        (not= (comparator r1) (comparator r2)))
                      (constantly false))
          update-row (fn [acc row fns+cols]
                       (reduce (fn [acc [f col]]
                                 (update acc (columns col)
                                         #(f % (get row (columns col)))))
                               acc fns+cols))
          [start-row end-row] (map #(vec (repeat (count (first rows)) %)) [::start ::end])
          start-fns+cols (for [col group-cols] [(fn [_ v] v) col])
          remove-starts (fn [acc] (into [] (remove (partial = ::start)) acc))
          lazy-agg
          (fn lazy-agg [[r1 & more :as rem-rows] acc]
            (cond (empty? rem-rows) (cond-> []
                                      (not= acc start-row)
                                      (conj (remove-starts (update-row acc end-row ffa))))
                  (empty? acc) (recur rem-rows (update-row start-row r1 start-fns+cols))
                  (excluded? acc r1) (cons (remove-starts (update-row acc end-row ffa))
                                           (lazy-seq (lazy-agg rem-rows [])))
                  :else (recur more (update-row acc r1 ffa))))
          old->new-keys (into {} (map (comp vec rest)) ffa)]
      {:__result__
       [(into (array-map)
              (comp
               (filter (into #{} cat [group-cols (mapv second ffa)]))
               (map-indexed (fn [i k]
                              [(if-let [new-k (old->new-keys k)] new-k k) i])))
              (keys columns))
        (lazy-seq (lazy-agg rows []))]})))

;; NOTE: Beware of multiple references to queries on plan nodes to avoid OOM
(defn execute
  [plan-nodes]
  (let [{[columns rows] :__result__ resources :__resources__}
        (reduce
         (fn [acc v]
           (if (fn? v) (merge acc (v acc)) (set/rename-keys acc {:__result__ v})))
         {} (flatten (map reverse (partition 2 plan-nodes))))]
    (doall rows)
    (doseq [^Closeable resource resources]
      (.close resource))
    (file-data->maps (keys columns) rows)))
