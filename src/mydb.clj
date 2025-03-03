(ns mydb
  (:refer-clojure :exclude [and or sort merge < >])
  (:require
   [clojure.core :as core]
   [clojure.data.csv :as csv]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :refer [rename-keys]])
  (:import
   (java.io Closeable FileInputStream RandomAccessFile)))

(set! *warn-on-reflection* true)

(def ^:private page-size 4096)
(def ^:private free-offset-size 2)
(def ^:private count-size 2)
(def ^:private static-meta-size (+ free-offset-size count-size))
(def ^:private start-offset-size 2)
(def ^:private record-id-size 2)
(def ^:private slot-size (+ start-offset-size record-id-size))
(def ^:private page-directory-entry-size 2)
(def ^:private page-directory-entries (/ page-size page-directory-entry-size))

(defn mask-first-byte ^long
  [x]
  (bit-and x 0xff))

(defn count->unsigned-short-arr
  [x]
  [(mask-first-byte (bit-shift-right x 8)) (mask-first-byte x)])

(defn unsigned-short-arr->count ^long
  [[high low]]
  (bit-or (bit-shift-left (mask-first-byte  high) 8) (mask-first-byte low)))

(defn- insert-page-metadata [rows]
  (let [static-meta (mapcat count->unsigned-short-arr [(count rows) (count (flatten rows))])]
    (concat
     [rows]
     (map concat (map count->unsigned-short-arr (reductions + (cons 0 (butlast (map (comp count flatten) rows)))))
          (map count->unsigned-short-arr (iterate inc 1)))
     static-meta)))

(defn- flatten-page [page-data]
  (let [flat-rows (flatten (first page-data))
        flat-metadata (flatten (rest page-data))]
    (concat flat-rows (repeat (- page-size (count flat-rows) (count flat-metadata)) 0) flat-metadata)))

(defn- build-page [rows]
  (->> (map (partial map (fn [^String s] (as-> (seq (.getBytes s)) $ (cons (count $) $)))) rows)
       (reduce (fn [[rows bytes-count :as acc] var-fields]
                 (let [new-count (+ bytes-count (count (flatten var-fields)) slot-size)]
                   (if (core/> new-count page-size)
                     (reduced acc)
                     [(conj rows var-fields) new-count])))
               [[] static-meta-size])
       first
       insert-page-metadata
       flatten-page
       byte-array))

(defn- write-rows
  ([writer rows] (write-rows writer rows 0 []))
  ([^RandomAccessFile writer rows page-num dir-entries]
   (cond
     (zero? (rem page-num (inc page-directory-entries)))
     (recur writer rows (inc page-num) dir-entries)
     (not-empty rows)
     (let [^bytes binary-page (build-page rows)
           static-meta (take-last static-meta-size binary-page)
           row-n (unsigned-short-arr->count (take 2 static-meta))
           free-offset (unsigned-short-arr->count (take-last free-offset-size static-meta))
           free-bytes (count->unsigned-short-arr (- (- page-size static-meta-size (* row-n slot-size)) free-offset))]
       (.seek writer (* page-num page-size))
       (.write writer binary-page)
       (recur writer (drop row-n rows) (inc page-num) (conj dir-entries [free-bytes])))
     :else dir-entries)))

(defn- write-page-dirs
  ([dir-entries writer] (write-page-dirs dir-entries writer 0))
  ([dir-entries ^RandomAccessFile writer page-num]
   (when-let [data (not-empty (flatten (take page-directory-entries dir-entries)))]
     (.seek writer (* page-num page-size))
     (.write writer (byte-array (concat data (repeat (- page-size (count data)) 0xFF))))
     (recur (drop (count data) dir-entries) writer (+ page-num (inc page-directory-entries))))))

(defn create-table-from-csv [table-name]
  (let [csv-table-name (str table-name "_table.csv")
        catalog-file-name (str table-name "_catalog.edn")
        heapfile-table-name (str table-name "_table.cljdb")]
    (with-open [table-reader (io/reader csv-table-name)]
      (let [columns (mapv keyword (first (csv/read-csv table-reader)))]
        (spit catalog-file-name {:columns columns})
        (spit heapfile-table-name "")
        (with-open [writer (RandomAccessFile. heapfile-table-name "rw")]
          (-> (write-rows writer (csv/read-csv table-reader))
              (write-page-dirs writer)))))))

(comment
  (create-table-from-csv "person")
  (create-table-from-csv "dog")
  (create-table-from-csv "movies")
  (create-table-from-csv "tags")
  (future (create-table-from-csv "ratings")))
; Query executor that takes a parsed query plan made up of plan nodes assigned to query keys and executes it retrieving data from csv files.
; It makes use of lazy sequences as an interface between query plan nodes to avoid fetching all the data when it is not necessary.
; It supports the following query plan nodes:
; - Scan: Creates a lazy sequence of maps whose keys correspond to the values of each of the rows provided by a specified csv file
; - Projection: Selects columns from the intermediate result and returns them
; - Selection: Filters rows based on max 2 conditions which can be combined with 'AND' or 'OR'
; - Limit: Limits the number of results
; - Sort: Sorts in ascending order by the given fields
; - Merge: Merge the result of many queries (Like union in SQL)
(defn- drop-nth [n coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (concat (take (dec n) (rest s))
             (drop-nth n (drop n s))))))

(defn- stringify [^bytes bytes]
  (let [length (alength bytes)
        result (transient [])]
    (loop [i 0 res result]
      (if (core/< i length)
        (let [segment-len (mask-first-byte (aget bytes i))]
          (recur (+ i 1 segment-len)
                 (conj! res (String. bytes ^long (inc i) segment-len))))
        (persistent! res)))))

(defn- read-heap-file [^FileInputStream reader col-num]
  (mapcat (fn [^bytes page]
            (->> (unsigned-short-arr->count (take-last free-offset-size page))
                 (java.util.Arrays/copyOfRange page 0)
                 stringify
                 (partition col-num)))
          (drop-nth (inc page-directory-entries)
                    (take-while some?
                                (repeatedly #(let [arr (byte-array page-size)]
                                               (when (pos? (.read reader arr)) arr)))))))

(defn- file-data->maps [columns heap-file-data]
  (let [columns-struct (apply create-struct columns)]
    (map (partial apply struct columns-struct)
         heap-file-data)))

(defn heap-file-scan [table]
  (fn [_]
    (let [table-reader (FileInputStream. (str table "_table.cljdb"))
          catalog-reader (java.io.PushbackReader. (io/reader (str table "_catalog.edn")))
          columns (:columns (edn/read catalog-reader))]
      {:__result__ (file-data->maps columns (read-heap-file table-reader (count columns)))
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

(defn merge
  [& tables]
  (fn [{:keys [__result__] :as iresultset}]
    {:__result__ (apply concat __result__ (map (fn [table] (table iresultset)) tables))}))

(defn execute
  ([plan-nodes] (execute plan-nodes {}))
  ([[current-key current-plan-seq :as plan-nodes] iresultset]
   (let [res (reduce (fn [acc plan-fn] (core/merge acc (plan-fn acc))) iresultset current-plan-seq)]
     (doseq [^Closeable resource (:__resources__ res)]
       (doall (:__result__ res))
       (.close resource))
     (if-let [next-plan-nodes (nnext plan-nodes)]
       (recur next-plan-nodes (rename-keys res {:__result__ current-key}))
       (:__result__ res)))))

