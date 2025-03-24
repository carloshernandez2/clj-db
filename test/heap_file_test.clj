(ns heap-file-test
  (:require
   [clojure.data.csv :as csv]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [heap-file :refer [create-table-from-csv scan write-rows partition-rows empty-page-directory index-page-directory count->unsigned-short-arr page-size]])
  (:import
   [java.io PushbackReader RandomAccessFile]))

(def dog-table "dog")
(def row ["a" "b" "cd"])
(def full-page-directory (mapcat identity (repeat 0 (count->unsigned-short-arr page-size))))
(def used-page-directory (concat (count->unsigned-short-arr 9) (drop 2 empty-page-directory)))

(deftest partition-rows-test
  (testing "Distributes rows across indexes until no rows are left in empty page dir"
    (let [rows (repeat 1000 row)
          result (partition-rows (index-page-directory empty-page-directory) rows)]
      (is (= (count rows) (count (mapcat identity (vals result)))))))

  (testing "Empty result when page directory is full"
    (is (= {} (partition-rows (index-page-directory full-page-directory)
                              [row]))))

  (testing "Does not put more data that an index can handle"
    (is (= {1 [row] 2 [row]} (partition-rows (index-page-directory used-page-directory)
                                             (repeat 2 row))))))

(deftest write-rows-test
  (testing "Inserts row at the end"
    (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [col-num (count (:columns (edn/read catalog-reader)))
            rows (vec (scan heap-file-stream col-num))
            new-row (first rows)]
        (write-rows heap-file-stream [new-row])
        (is (= (conj rows new-row) (scan heap-file-stream col-num))))))

  (testing "Inserts many rows at the end"
    (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [col-num (count (:columns (edn/read catalog-reader)))
            rows (scan heap-file-stream col-num)]
        (write-rows heap-file-stream rows)
        (is (= (concat rows rows) (scan heap-file-stream col-num))))))

  (testing "Surpasses page boundaries without breaking"
    (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [col-num (count (:columns (edn/read catalog-reader)))
            rows (scan heap-file-stream col-num)
            new-rows (mapcat identity (repeat 100 rows))]
        (write-rows heap-file-stream new-rows)
        (is (= (concat rows new-rows) (scan heap-file-stream col-num)))))))

(deftest create-table-from-csv-test
  (testing "Created heap file contains same data as CSV file"
    (with-open [csv-reader (io/reader (str dog-table "_table.csv"))
                heap-file-reader (RandomAccessFile. (str dog-table "_table.cljdb") "r")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [csv-rows (rest (csv/read-csv csv-reader))
            col-num (count (:columns (edn/read catalog-reader)))]
        (create-table-from-csv dog-table)
        (is (= csv-rows (scan heap-file-reader col-num)))))))

(comment
  (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")]
    (write-rows heap-file-stream [["a" "b" "c" "d" "e"]]))
  (with-open [heap-file-stream (RandomAccessFile. (str "tags" "_table.cljdb") "rw")]
    (write-rows heap-file-stream [["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                                   "cccccccccccccccccccccccccccccccccccccccccccccccc"
                                   "dddddddddddddddddddddddddddddddddddddddddd"]]))
  (with-open [heap-file-stream (RandomAccessFile. (str "ratings" "_table.cljdb") "rw")]
    (write-rows heap-file-stream [["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                                   "cccccccccccccccccccccccccccccccccccccccccccccccc"
                                   "dddddddddddddddddddddddddddddddddddddddddd"]])))
