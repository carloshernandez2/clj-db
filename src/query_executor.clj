(ns query-executor
  (:refer-clojure :exclude [and or sort < >])
  (:require
   [clojure.core :as core]
   [clojure.data.csv :as csv]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :refer [rename-keys] :as set]
   [heap-file :as heap-file])
  (:import
   (java.io Closeable RandomAccessFile)))

; Query executor that takes a parsed query plan made up of plan nodes assigned to query keys and executes it retrieving data from csv files.
; It makes use of lazy sequences as an interface between query plan nodes to avoid fetching all the data when it is not necessary.
; It supports the following query plan nodes:
; - Scan: Creates a lazy sequence of maps whose keys correspond to the values of each of the rows provided by a specified csv file
; - Projection: Selects columns from the intermediate result and returns them
; - Selection: Filters rows based on max 2 conditions which can be combined with 'AND' or 'OR'
; - Limit: Limits the number of results
; - Sort: Sorts in ascending order by the given fields
; - Nested Loops Join: Carthesian product of two tables to which a filter is applied (JOIN in SQL)
; - Hash Join: Builds a hash map from one table and uses it to find matching rows in the other. Only applicable for equijoins.
; - Sort-merge Join: Assumes sorted inputs and creates a cartisian product by traversing tables only once. Only applicable for equijoins.

(set! *warn-on-reflection* true)

(defn- file-data->maps [columns heap-file-data]
  (map (partial zipmap columns) heap-file-data))

(defn heap-file-scan [table]
  (fn [{resources :__resources__}]
    (let [table-reader (RandomAccessFile. (str table "_table.cljdb") "r")
          catalog-reader (java.io.PushbackReader. (io/reader (str table "_catalog.edn")))
          columns  (into (array-map) (map-indexed (fn [idx val] [val idx])
                                                  (:columns (edn/read catalog-reader))))]
      {:__result__ [columns (heap-file/scan table-reader (count columns))]
       :__resources__ (concat resources [table-reader catalog-reader])})))

(defn csv-scan
  [table]
  (fn [{resources :__resources__}]
    (let [table-reader (io/reader (str table "_table.csv"))
          catalog-reader (java.io.PushbackReader. (io/reader (str table "_catalog.edn")))
          columns (into (array-map) (map-indexed (fn [idx val] [val idx])
                                                 (:columns (edn/read catalog-reader))))]
      {:__result__ [columns (rest (csv/read-csv table-reader))]
       :__resources__ (concat resources [table-reader catalog-reader])})))

(defn projection
  [& cols]
  (fn [{[columns rows] :__result__}]
    (let [indexed-cols (into (array-map) (filter (comp (set cols) first)) columns)]
      {:__result__
       [indexed-cols
        (map (fn [row]
               (into [] (keep-indexed #(when ((set/map-invert indexed-cols) %1) %2)) row)) rows)]})))

(defn and [res1 res2]
  (core/and res1 res2))

(defn or [res1 res2]
  (core/or res1 res2))

;TODO: Delete when schema is supported
(defn > [n1 n2]
  (pos? (compare n1 n2)))

(defn < [n1 n2]
  (neg? (compare n1 n2)))
;------------------------------------

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

(defn sort
  [& fields]
  (fn [{[columns rows] :__result__}]
    {:__result__
     [columns
      (sort-by (apply juxt (map (fn [field] #(get % (columns field))) fields)) rows)]}))

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
          (map (fn [row1] (vec (concat row1 row2))) (hash-table field)))))) t-name))

(defn- partition-by-matches
  [[r1 :as s1] f1 [r2 :as s2] f2]
  (let [v1 (f1 r1)
        v2 (f2 r2)]
    (cond (not (or r1 r2)) []
          (= v1 v2) (cons
                     [(take-while #(= v1 (f1 %)) s1) (take-while #(= v2 (f2 %)) s2)]
                     (lazy-seq (partition-by-matches
                                (drop-while #(= v1 (f1 %)) s1) f1
                                (drop-while #(= v2 (f2 %)) s2) f2)))
          :else (let [vmin (if (< v1 v2) v1 v2)]
                  (recur (drop-while #(= vmin (f1 %)) s1) f1
                         (drop-while #(= vmin (f2 %)) s2) f2)))))

(defn sort-merge-join
  [[op k1 k2] t-name]
  (assert (= op =) "Sort merge join only works on equijoins")
  (base-join
   (fn [[t1-columns t1-rows] [t2-columns t2-rows]]
     (mapcat identity
             (map (fn [[g1 g2]] (for [row1 g1 row2 g2] (vec (concat row1 row2))))
                  (partition-by-matches t1-rows #(get % (t1-columns k1))
                                        t2-rows #(get % (t2-columns k2)))))) t-name))

(defn execute
  ([plan-nodes] (execute plan-nodes {}))
  ([[current-key current-plan-seq :as plan-nodes] iresultset]
   (let [res (reduce (fn [acc plan-fn] (merge acc (plan-fn acc))) iresultset current-plan-seq)]
     (if-let [next-plan-nodes (nnext plan-nodes)]
       (recur next-plan-nodes (rename-keys res {:__result__ current-key}))
       (let [[columns rows] (:__result__ res)]
         (doall rows)
         (doseq [^Closeable resource (:__resources__ res)]
           (.close resource))
         (file-data->maps (keys columns) rows))))))
