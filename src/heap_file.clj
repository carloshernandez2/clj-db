(ns heap-file
  (:refer-clojure :exclude [read])
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io])
  (:import
   [java.io FileInputStream RandomAccessFile]))

(set! *warn-on-reflection* true)

(def page-size 4096)
(def free-offset-size 2)
(def count-size 2)
(def static-meta-size (+ free-offset-size count-size))
(def start-offset-size 2)
(def record-id-size 2)
(def slot-size (+ start-offset-size record-id-size))
(def page-directory-entry-size 2)
(def page-directory-entries (/ page-size page-directory-entry-size))

(defn mask-first-byte ^long
  [x]
  (bit-and x 0xff))

(defn count->unsigned-short-arr
  [x]
  [(mask-first-byte (bit-shift-right x 8)) (mask-first-byte x)])

(defn unsigned-short-arr->count ^long
  [[high low]]
  (bit-or (bit-shift-left (mask-first-byte  high) 8) (mask-first-byte low)))

(defn- drop-nth [n coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (concat (take (dec n) (rest s))
             (drop-nth n (drop n s))))))

(defn- stringify [^bytes bytes]
  (let [length (alength bytes)
        result (transient [])]
    (loop [i 0 res result]
      (if (< i length)
        (let [segment-len (mask-first-byte (aget bytes i))]
          (recur (+ i 1 segment-len)
                 (conj! res (String. bytes (inc i) segment-len))))
        (persistent! res)))))

(defn read [^FileInputStream reader col-num]
  (mapcat (fn [^bytes page]
            (->> (unsigned-short-arr->count (take-last free-offset-size page))
                 (java.util.Arrays/copyOfRange page 0)
                 stringify
                 (partition col-num)))
          (drop-nth (inc page-directory-entries)
                    (take-while some?
                                (repeatedly #(let [arr (byte-array page-size)]
                                               (when (pos? (.read reader arr)) arr)))))))

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

(defn- build-page ^bytes [rows]
  (->> (map (partial map (fn [^String s] (as-> (seq (.getBytes s)) $ (cons (count $) $)))) rows)
       (reduce (fn [[rows bytes-count :as acc] var-fields]
                 (let [new-count (+ bytes-count (count (flatten var-fields)) slot-size)]
                   (if (> new-count page-size)
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
     (let [binary-page (build-page rows)
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

;- Create map (page-index, data-to-insert-with-meta and empty space)
;- Insert data in corresponding pages starting at free offset
(defn insert-rows [table-name rows])

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
