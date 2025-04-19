(ns heap-file-test
  (:require
   [clojure.data.csv :as csv]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [heap-file :refer [create-table-from-csv empty-page-directory
                      page-directory-entries-num parse-page-directory
                      section-rows scan short->bytes string-row->types
                      write-rows]])
  (:import
   [java.io PushbackReader RandomAccessFile]))

(def dog-table "dog")
(def row ["a" "b" "cd"])
(def byte-row [[1 97] [1 98] [2 99 100]])
(def full-page-directory (repeat page-directory-entries-num 0))
(def used-page-directory (concat (seq (short->bytes 9)) (drop 2 empty-page-directory)))
(def fake-catalog {:schema [:string :string :string]})

(deftest partition-rows-test
  (testing "Distributes rows across indexes until no rows are left in empty page dir"
    (let [rows (repeat 1000 row)
          [result] (section-rows
                    (parse-page-directory empty-page-directory) fake-catalog rows)]
      (is (= (count rows) (count (mapcat identity (vals result)))))))

  (testing "Empty result when page directory is full"
    (is (= [{} [row]] (section-rows
                       (parse-page-directory full-page-directory) fake-catalog [row]))))

  (testing "Does not put more data that an index can handle"
    (is (= [{1 [byte-row] 2 [byte-row]} []]
           (section-rows
            (parse-page-directory used-page-directory) fake-catalog (repeat 2 row))))))

(deftest write-rows-test
  (testing "Inserts row at the end"
    (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [catalog (edn/read catalog-reader)
            rows (vec (scan catalog heap-file-stream))
            new-row (first rows)]
        (write-rows heap-file-stream catalog [new-row])
        (is (= (conj rows new-row) (scan catalog heap-file-stream))))))

  (testing "Inserts many rows at the end"
    (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [catalog (edn/read catalog-reader)
            rows (scan catalog heap-file-stream)]
        (write-rows heap-file-stream catalog rows)
        (is (= (concat rows rows) (scan catalog heap-file-stream))))))

  (testing "Surpasses page boundaries without breaking"
    (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [catalog (edn/read catalog-reader)
            rows (scan catalog heap-file-stream)
            new-rows (mapcat identity (repeat 100 rows))]
        (write-rows heap-file-stream catalog new-rows)
        (is (= (concat rows new-rows) (scan catalog heap-file-stream)))))))

(deftest create-table-from-csv-test
  (testing "Created heap file contains same data as CSV file"
    (with-open [csv-reader (io/reader (str dog-table "_table.csv"))
                heap-file-reader (RandomAccessFile. (str dog-table "_table.cljdb") "r")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (create-table-from-csv dog-table {:schema [:string :int :string :string :string]})
      (let [catalog (edn/read catalog-reader)
            csv-rows (map (partial string-row->types catalog)
                          (rest (csv/read-csv csv-reader)))]
        (is (= csv-rows (scan catalog heap-file-reader)))))))

(comment
  (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")
              catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
    (write-rows heap-file-stream (edn/read catalog-reader) [["a" "b" "c" "d" "e"]]))
  (with-open [heap-file-stream (RandomAccessFile. (str "tags" "_table.cljdb") "rw")
              catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
    (write-rows heap-file-stream (edn/read catalog-reader)
                [["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                  "cccccccccccccccccccccccccccccccccccccccccccccccc"
                  "dddddddddddddddddddddddddddddddddddddddddd"]]))
  (with-open [heap-file-stream (RandomAccessFile. (str "ratings" "_table.cljdb") "rw")
              catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
    (write-rows heap-file-stream (edn/read catalog-reader)
                [["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                  "cccccccccccccccccccccccccccccccccccccccccccccccc"
                  "dddddddddddddddddddddddddddddddddddddddddd"]])))
