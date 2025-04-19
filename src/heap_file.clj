(ns heap-file
  (:refer-clojure :exclude [read])
  (:require
   [clojure.data.csv :as csv]
   [clojure.java.io :as io])
  (:import
   [java.io RandomAccessFile]
   [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(defn to-bytes ^bytes [b] (if (bytes? b) b (byte-array b)))

(defn value->bytes [f size v]
  (let [buf (ByteBuffer/allocate size)]
    (f buf v)
    (seq (.array buf))))

(defn bytes->value [f b]
  (f (ByteBuffer/wrap (to-bytes b))))

(defn short->bytes [v]
  (value->bytes (memfn ^ByteBuffer putShort ^Short n) Short/BYTES (short v)))

(defn float->bytes [v]
  (value->bytes (memfn ^ByteBuffer putFloat ^Float n) Float/BYTES (float v)))

(defn int->bytes [v]
  (value->bytes (memfn ^ByteBuffer putInt ^Integer n) Integer/BYTES (int v)))

(defn bytes->short [b]
  (bytes->value (memfn ^ByteBuffer getShort) b))

(defn bytes->float [b]
  (bytes->value (memfn ^ByteBuffer getFloat) b))

(defn bytes->int [b]
  (bytes->value (memfn ^ByteBuffer getInt) b))

(def page-size 4096)
(def empty-data-page (byte-array (repeat page-size 0)))
(def free-offset-size 2)
(def count-size 2)
(def static-meta-size (+ free-offset-size count-size))
(def slot-size 2)
(def page-directory-entry-size 2)
(def page-directory-entries-num (/ page-size page-directory-entry-size))
(def empty-page-directory
  (mapcat identity (repeat page-directory-entries-num
                           (seq (short->bytes (- page-size static-meta-size))))))

(defn string-row->types [{:keys [schema]} row]
  (mapv (fn [type v]
          (case type
            :string v
            :float (Float/parseFloat v)
            :int (Integer/parseInt v))) schema row))

(defn- row->bytes [{:keys [schema]} row]
  (mapv (fn [type v]
          (case type
            :string (let [b (.getBytes ^String v)] (cons (count b) b))
            :float (float->bytes v)
            :int (int->bytes v))) schema row))

(defn- bytes->row [{:keys [schema]} row]
  (mapv (fn [type v]
          (case type
            :string (let [b (to-bytes v)] (String. b 1 (dec (alength b))))
            :float (bytes->float v)
            :int (bytes->int v))) schema row))

(defn- group-bytes [{:keys [schema]} ^bytes bytes]
  (let [length (alength bytes)
        result (transient [])]
    (loop [i 0 res result schema (mapcat identity (repeat schema))]
      (if (< i length)
        (let [segment-len (case (first schema)
                            :string (int (inc (Byte/toUnsignedInt (aget bytes i))))
                            :float Float/BYTES
                            :int Integer/BYTES)]
          (recur (+ i segment-len)
                 (conj! res (seq (java.util.Arrays/copyOfRange
                                  bytes i (+ i segment-len))))
                 (rest schema)))
        (persistent! res)))))

(defn read [^RandomAccessFile reader [i1 & rem-indexes]]
  (lazy-seq
   (when i1
     (let [arr (byte-array page-size)
           res (do (.seek reader (* page-size i1)) (.read reader arr))]
       (when (pos? res) (cons arr (read reader rem-indexes)))))))

(defn- take-data-rows [{:keys [columns] :as catalog} ^bytes page]
  (let [page-length (alength page)]
    (->> (bytes->short (java.util.Arrays/copyOfRange page (- page-length 2) page-length))
         int
         (java.util.Arrays/copyOfRange page 0)
         (group-bytes catalog)
         (partition (count columns))
         (sequence (map vec)))))

(defn scan [catalog reader]
  (mapcat #(sequence (map (partial bytes->row catalog)) (take-data-rows catalog %))
          (read reader (filter #(pos? (rem % (inc page-directory-entries-num))) (range)))))

(defn- conj-page-metadata [rows]
  (let [static-meta (mapcat short->bytes [(count rows) (count (flatten rows))])]
    (concat
     [rows]
     (mapcat short->bytes
             (reductions + (cons 0 (drop-last (map (comp count flatten) rows)))))
     static-meta)))

(defn- flatten-page [page-data]
  (let [flat-rows (flatten (first page-data))
        flat-metadata (flatten (rest page-data))]
    (concat flat-rows (repeat (- page-size (count flat-rows) (count flat-metadata)) 0) flat-metadata)))

(defn- build-page ^bytes [catalog rows current-page]
  (->> (concat (take-data-rows catalog current-page) rows)
       conj-page-metadata
       flatten-page
       byte-array))

(defn parse-page-directory [page-directory]
  (mapv bytes->short (partition 2 page-directory)))

(defn section-rows [page-directory catalog rows]
  (subvec
   (reduce
    (fn [[result rows] [idx free-count]]
      (transduce
       (comp (map (partial row->bytes catalog)) (map (fn [row] [row (+ (count (flatten row)) slot-size)])))
       (completing
        (fn [[result remaining acc-count :as all] [bytes row-count]]
          (let [new-count (+ acc-count row-count)]
            (if (> new-count free-count)
              (reduced all)
              [(update result idx (fnil #(conj % bytes) [])) (rest remaining) new-count]))))
       [result rows 0] rows))
    [(sorted-map) rows]
    (partition 2 (interleave (iterate inc 1) page-directory))) 0 2))

(defn write-rows
  ([stream catalog rows] (write-rows stream catalog rows 0))
  ([^RandomAccessFile stream catalog rows section-num]
   (let [start-page-num (* section-num (inc page-directory-entries-num))
         page-directory (parse-page-directory (or (first (read stream [start-page-num])) empty-page-directory))
         [rows-per-index missing-rows] (section-rows page-directory catalog rows)
         new-page-directory (byte-array
                             (sequence (comp
                                        (map-indexed
                                         (fn [idx current-count]
                                           (let [rows (get rows-per-index (inc idx) [])]
                                             (- current-count
                                                (count (flatten rows))
                                                (* (count rows) slot-size)))))
                                        (map short->bytes)
                                        cat) page-directory))
         data-to-insert (vals rows-per-index)
         indexes (map (partial + start-page-num) (keys rows-per-index))]
     (.seek stream (* start-page-num page-size))
     (.write stream new-page-directory)
     (doseq [[idx rows content] (->> (concat (read stream indexes) (repeat []))
                                     (interleave indexes data-to-insert)
                                     (partition 3))]
       (.seek stream (* idx page-size))
       (.write stream (build-page catalog rows (or (not-empty content) empty-data-page))))
     (when-not (empty? missing-rows)
       (recur stream catalog missing-rows (inc section-num))))))

(defn create-table-from-csv [table-name config]
  (let [csv-table-name (str table-name "_table.csv")
        catalog-file-name (str table-name "_catalog.edn")
        heapfile-table-name (str table-name "_table.cljdb")]
    (with-open [table-reader (io/reader csv-table-name)
                writer (RandomAccessFile. heapfile-table-name "rw")]
      (let [columns (mapv keyword (first (csv/read-csv table-reader)))
            catalog (merge config {:columns columns})]
        (spit catalog-file-name catalog)
        (spit heapfile-table-name "")
        (write-rows writer catalog (map (partial string-row->types catalog)
                                        (csv/read-csv table-reader)))))))

(comment
  (create-table-from-csv "person" {:schema [:string :int :string :string]})
  (create-table-from-csv "dog" {:schema [:string :int :string :string :string]})
  (create-table-from-csv "movies" {:schema [:int :string :string]})
  (create-table-from-csv "tags" {:schema [:int :int :string :int]})
  (future (create-table-from-csv "ratings" {:schema [:int :int :float :int]})))
