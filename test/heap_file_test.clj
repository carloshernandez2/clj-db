(ns heap-file-test
  (:require
   [clojure.data.csv :as csv]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [heap-file :refer [create-table-from-csv scan write-rows]]) 
  (:import
   [java.io PushbackReader RandomAccessFile]))

(def dog-table "dog")

(deftest write-rows-test
  (testing "Inserts row at the end"
    (with-open [heap-file-stream (RandomAccessFile. (str dog-table "_table.cljdb") "rw")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [col-num (count (:columns (edn/read catalog-reader)))
            rows (vec (scan heap-file-stream col-num))]
        (write-rows heap-file-stream (take 1 rows))
        (is (= (conj rows (first rows)) (scan heap-file-stream col-num)))))))

(deftest create-table-from-csv-test
  (testing "Created heap file contains same data as CSV file"
    (with-open [csv-reader (io/reader (str dog-table "_table.csv"))
                heap-file-reader (RandomAccessFile. (str dog-table "_table.cljdb") "r")
                catalog-reader (PushbackReader. (io/reader (str dog-table "_catalog.edn")))]
      (let [csv-rows (rest (csv/read-csv csv-reader))
            col-num (count (:columns (edn/read catalog-reader)))]
        (create-table-from-csv dog-table)
        (is (= csv-rows (scan heap-file-reader col-num)))))))

