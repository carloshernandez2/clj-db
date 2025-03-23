(ns heap-file
  (:refer-clojure :exclude [read])
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io])
  (:import
   [java.io RandomAccessFile]))

(set! *warn-on-reflection* true)

(defn mask-first-byte ^long
  [x]
  (bit-and x 0xff))

(defn count->unsigned-short-arr
  [x]
  [(mask-first-byte (bit-shift-right x 8)) (mask-first-byte x)])

(defn unsigned-short-arr->count ^long
  [[high low]]
  (bit-or (bit-shift-left (mask-first-byte  high) 8) (mask-first-byte low)))

(def page-size 4096)
(def empty-data-page (byte-array (repeat page-size 0)))
(def free-offset-size 2)
(def count-size 2)
(def static-meta-size (+ free-offset-size count-size))
(def start-offset-size 2)
(def record-id-size 2)
(def slot-size (+ start-offset-size record-id-size))
(def page-directory-entry-size 2)
(def page-directory-entries-num (/ page-size page-directory-entry-size))
(def empty-page-directory
  (mapcat identity (repeat page-directory-entries-num
                           (count->unsigned-short-arr (- page-size static-meta-size)))))

(defn- stringify [^bytes bytes]
  (let [length (alength bytes)
        result (transient [])]
    (loop [i 0 res result]
      (if (< i length)
        (let [segment-len (mask-first-byte (aget bytes i))]
          (recur (+ i 1 segment-len)
                 (conj! res (String. bytes (inc i) segment-len))))
        (persistent! res)))))

(defn read [^RandomAccessFile reader [i1 & rem-indexes]]
  (lazy-seq
   (when i1
     (let [arr (byte-array page-size)
           res (do (.seek reader (* page-size i1)) (.read reader arr))]
       (when (pos? res) (cons arr (read reader rem-indexes)))))))

(comment
  (with-open [stream (RandomAccessFile. "dog_table.cljdb" "r")]
    (read stream [9])))

(defn- take-data-rows [col-num ^bytes page]
  (->> (unsigned-short-arr->count (take-last free-offset-size page))
       (java.util.Arrays/copyOfRange page 0)
       stringify
       (partition col-num)))

(defn scan [reader col-num]
  (mapcat (partial take-data-rows col-num)
          (read reader (filter #(pos? (rem % (inc page-directory-entries-num))) (range)))))

(defn- data->bytes [row]
  (map #(let [b (.getBytes ^String %)] (cons (count b) b)) row))

;TODO: Static meta should not be here, it wastes some bytes that can't be filled 
(defn- insertable-rows [bytes-available rows]
  (loop [acc []
         bytes-count 0
         remaining rows]
    (let [current-row (first remaining)
          bytes (data->bytes current-row)
          new-count (+ bytes-count (count (flatten bytes)) slot-size)]
      (if (or (empty? remaining) (> new-count bytes-available))
        acc
        (recur (conj acc current-row) new-count (rest remaining))))))

(defn- conj-page-metadata [rows]
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

(defn- build-page ^bytes [rows current-page]
  (->> (concat (take-data-rows (count (first rows)) current-page) rows)
       (map data->bytes)
       conj-page-metadata
       flatten-page
       byte-array))

(defn index-page-directory [page-directory]
  (into (sorted-map)
        (keep-indexed
         (fn [idx entry]
           (let [free-count (unsigned-short-arr->count entry)]
             [(inc idx) free-count])))
        (partition 2 page-directory)))

(defn partition-rows [indexed-page-directory rows]
  (loop [indexed-entries indexed-page-directory
         rows rows
         result (sorted-map)]
    (if (or (empty? rows) (empty? indexed-entries))
      result
      (let [[[idx free-count] & rest-indexed] indexed-entries
            data-to-insert (insertable-rows free-count rows)]
        (recur rest-indexed
               (drop (count data-to-insert) rows)
               (cond-> result (not-empty data-to-insert) (assoc idx data-to-insert)))))))

(defn write-rows
  ([stream rows] (write-rows stream rows 0))
  ([^RandomAccessFile stream rows section-num]
   (let [start-page-num (* section-num (inc page-directory-entries-num))
         page-directory (or (first (read stream [start-page-num])) empty-page-directory)
         indexed-page-directory (index-page-directory page-directory)
         rows-per-index (partition-rows indexed-page-directory rows)
         indexed-added-byte-count (into (sorted-map)
                                        (map (fn [[idx rows]]
                                               [idx (+ (count (flatten (map data->bytes rows)))
                                                       (* (count rows) slot-size))]))
                                        rows-per-index)
         new-page-directory (->> indexed-added-byte-count
                                 (merge-with - indexed-page-directory)
                                 vals
                                 (mapv count->unsigned-short-arr)
                                 (mapcat identity)
                                 byte-array)
         data-to-insert (vals rows-per-index)
         indexes (map (partial + start-page-num) (keys rows-per-index))]
     (.seek stream (* start-page-num page-size))
     (.write stream new-page-directory)
     (doseq [[idx rows content] (->> (concat (read stream indexes) (repeat []))
                                     (interleave indexes data-to-insert)
                                     (partition 3))]
       (.seek stream (* idx page-size))
       (.write stream (build-page rows (or (not-empty content) empty-data-page))))
     (when-let [missing-rows (not-empty (drop (count (mapcat identity data-to-insert)) rows))]
       (recur stream missing-rows (inc section-num))))))

(defn create-table-from-csv [table-name]
  (let [csv-table-name (str table-name "_table.csv")
        catalog-file-name (str table-name "_catalog.edn")
        heapfile-table-name (str table-name "_table.cljdb")]
    (with-open [table-reader (io/reader csv-table-name)
                writer (RandomAccessFile. heapfile-table-name "rw")]
      (let [columns (mapv keyword (first (csv/read-csv table-reader)))]
        (spit catalog-file-name {:columns columns})
        (spit heapfile-table-name "")
        (write-rows writer (csv/read-csv table-reader))))))

(comment
  #_{:clj-kondo/ignore [:unresolved-namespace]}
  (user/run-tests)
  (create-table-from-csv "person")
  (create-table-from-csv "dog")
  (create-table-from-csv "movies")
  (create-table-from-csv "tags")
  (future (create-table-from-csv "ratings")))
