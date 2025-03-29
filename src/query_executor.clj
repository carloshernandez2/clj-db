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

(set! *warn-on-reflection* true)

(defn- file-data->maps [columns heap-file-data]
  (map (partial zipmap columns) heap-file-data))

(defn heap-file-scan [table]
  (fn [_]
    (let [table-reader (RandomAccessFile. (str table "_table.cljdb") "r")
          catalog-reader (java.io.PushbackReader. (io/reader (str table "_catalog.edn")))
          columns (:columns (edn/read catalog-reader))]
      {:__result__ (file-data->maps columns (heap-file/scan table-reader (count columns)))
       :__resources__ [table-reader catalog-reader]})))

(defn csv-scan
  [table]
  (fn [_]
    (let [table-reader (io/reader (str table "_table.csv"))
          catalog-reader (java.io.PushbackReader. (io/reader (str table "_catalog.edn")))
          columns (:columns (edn/read catalog-reader))]
      {:__result__ (file-data->maps columns (rest (csv/read-csv table-reader)))
       :__resources__ [table-reader catalog-reader]})))

(defn projection
  [& cols]
  (fn [{:keys [__result__]}]
    {:__result__ (map (fn [row]
                        (select-keys row cols))
                      __result__)}))

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
  (fn [{:keys [__result__]}]
    {:__result__ (filter (fn [row]
                           (cond fn2 (expr (fn1 (field1 row) val1)
                                           (fn2 (field2 row) val2))
                                 :else (fn1 (field1 row) val1)))
                         __result__)}))

(defn limit
  [n]
  (fn [{:keys [__result__]}]
    {:__result__ (take n __result__)}))

(defn sort
  [& fields]
  (fn [{:keys [__result__]}]
    {:__result__ (sort-by (apply juxt fields) __result__)}))

(defn nested-loops-join
  [[op v1 v2] t-name]
  (fn [{:keys [__result__] :as iresultset}]
    {:__result__
     (let [t (t-name iresultset)
           duplicate-keys (seq (set/intersection (set (keys (first __result__)))
                                                 (set (keys (first t)))))
           name-mapping (->> duplicate-keys
                             (map #(keyword (name t-name) (name %)))
                             (zipmap duplicate-keys))
           renamed-t (map #(set/rename-keys % name-mapping) t)]
       (for [row1 __result__ row2 renamed-t
             :when (op (v1 row1) (v2 row2))]
         (merge row1 row2)))}))

(defn execute
  ([plan-nodes] (execute plan-nodes {}))
  ([[current-key current-plan-seq :as plan-nodes] iresultset]
   (let [res (reduce (fn [acc plan-fn] (merge acc (plan-fn acc))) iresultset current-plan-seq)]
     (doseq [^Closeable resource (:__resources__ res)]
       (doall (:__result__ res))
       (.close resource))
     (if-let [next-plan-nodes (nnext plan-nodes)]
       (recur next-plan-nodes (rename-keys res {:__result__ current-key}))
       (:__result__ res)))))

