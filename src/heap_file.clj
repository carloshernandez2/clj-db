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
(def empty-page-directory (mapcat identity (repeat page-directory-entries-num (count->unsigned-short-arr page-size))))

(defn- stringify [^bytes bytes]
  (let [length (alength bytes)
        result (transient [])]
    (loop [i 0 res result]
      (if (< i length)
        (let [segment-len (mask-first-byte (aget bytes i))]
          (recur (+ i 1 segment-len)
                 (conj! res (String. bytes (inc i) segment-len))))
        (persistent! res)))))

(defn read [^RandomAccessFile reader [i1 :as indexes]]
  (lazy-seq
   (let [arr (byte-array page-size)
         res (do (.seek reader (* page-size i1)) (.read reader arr))]
     (when (and (next indexes) (pos? res)) (cons arr (read reader (rest indexes)))))))

(defn- take-data-rows [col-num ^bytes page]
  (->> (unsigned-short-arr->count (take-last free-offset-size page))
       (java.util.Arrays/copyOfRange page 0)
       stringify
       (partition col-num)))

(defn scan [reader col-num]
  (mapcat (partial take-data-rows col-num)
          (read reader (filter #(pos? (rem % (inc page-directory-entries-num))) (range)))))

(defn- insertable-rows [rows]
  (reduce (fn [[rows bytes-count :as acc] row]
            (let [new-count (+ bytes-count (count (flatten row)) slot-size)]
              (if (> new-count page-size)
                (reduced acc)
                [(conj rows row) new-count])))
          [[] static-meta-size] rows))

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
       (map (partial map (fn [^String s] (as-> (seq (.getBytes s)) $ (cons (count $) $)))))
       insertable-rows
       first
       conj-page-metadata
       flatten-page
       byte-array))

(defn- write-section [^RandomAccessFile stream rows section]
  (let [start-page-num (* section (inc page-directory-entries-num))
        end-page-num (+ start-page-num page-directory-entries-num)
        content (read stream (range start-page-num (inc end-page-num)))]
    (loop [result-page-directory []
           current-data-pages (rest content)
           missing-rows rows
           page-num (inc start-page-num)]
      (if (or (empty? missing-rows) (> page-num end-page-num))
        (do
          (.seek stream (* start-page-num page-size))
          (.write stream (byte-array (concat result-page-directory (take (- page-size (count result-page-directory)) empty-page-directory))))
          missing-rows)
        (let [binary-page (build-page missing-rows (or (first current-data-pages) empty-data-page))
              static-meta (take-last static-meta-size binary-page)
              row-n (unsigned-short-arr->count (take 2 static-meta))
              free-offset (unsigned-short-arr->count (take-last free-offset-size static-meta))
              free-bytes (count->unsigned-short-arr
                          (- (- page-size static-meta-size (* row-n slot-size)) free-offset))]
          (.seek stream (* page-num page-size))
          (.write stream binary-page)
          (recur (concat result-page-directory free-bytes)
                 (rest current-data-pages)
                 (drop row-n missing-rows)
                 (inc page-num)))))))

;TODO: Create page-indexes to insert and assign data to them (to avoid having to read and write all pages unnecessarily)
(defn write-rows
  ([stream rows] (write-rows stream rows 0))
  ([^RandomAccessFile stream rows section-num]
   (when-let [missing-rows (not-empty (write-section stream rows section-num))]
     (recur stream missing-rows (inc section-num)))))

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
