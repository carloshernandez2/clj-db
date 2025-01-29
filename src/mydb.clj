(ns mydb
  (:refer-clojure :exclude [and or sort merge < >])
  (:require
   [clojure.core :as core]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [clojure.set :refer [rename-keys]]))

; Query executor that takes a parsed query plan made up of plan nodes assigned to query keys and executes it retrieving data from csv files.
; It makes use of lazy sequences as an interface between query plan nodes to avoid fetching all the data when it is not necessary.
; It supports the following query plan nodes:
; - Scan: Creates a lazy sequence of maps whose keys correspond to the values of each of the rows provided by a specified csv file
; - Projection: Selects columns from the intermediate result and returns them
; - Selection: Filters rows based on max 2 conditions which can be combined with 'AND' or 'OR'
; - Limit: Limits the number of results
; - Sort: Sorts in ascending order by the given fields
; - Merge: Merge the result of many queries (Like union in SQL)

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map keyword)
            repeat)
       (rest csv-data)))

(defn scan
  [table]
  (fn [_]
    (let [reader (io/reader table)]
      {:__result__ (csv-data->maps (csv/read-csv reader))
       :__resourses__ [reader]})))

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

(defn merge
  [& tables]
  (fn [{:keys [__result__] :as iresultset}]
    {:__result__ (apply concat __result__ (map (fn [table] (table iresultset)) tables))}))

(defn execute
  ([plan-nodes] (execute plan-nodes {}))
  ([[current-key current-plan-seq :as plan-nodes] iresultset]
   (let [res (reduce (fn [acc plan-fn] (core/merge acc (plan-fn acc))) iresultset current-plan-seq)]
     (doseq [resource (:__resources__ res)]
       (.close resource))
     (if-let [next-plan-nodes (nnext plan-nodes)]
       (recur next-plan-nodes (rename-keys res {:__result__ current-key}))
       (:__result__ res)))))

