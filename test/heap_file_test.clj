(ns heap-file-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [heap-file :refer [create-table-from-csv]]
   [query-executor :as q]))

(def dog-table "dog")

(def csv-query
  [:__result__ [(q/csv-scan dog-table)]])

(def heap-file-query
  [:__result__ [(q/heap-file-scan dog-table)]])

(deftest create-table-from-csv-test
  (testing "Created heap file contains same data as CSV file"
    (let [csv-rows (q/execute csv-query)]
      (create-table-from-csv dog-table)
      (is (= csv-rows (q/execute heap-file-query))))))
