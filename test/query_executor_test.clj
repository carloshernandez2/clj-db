(ns query-executor-test
  (:refer-clojure :exclude [and or sort < >])
  (:require
   [clojure.test :refer [deftest is]]
   [query-executor :refer [< > and csv-scan execute hash-join heap-file-scan
                           limit nested-loops-join or projection selection
                           sort sort-merge-join]])
  (:import
   [java.io Closeable]))

(set! *warn-on-reflection* true)

(def person-table "person")
(def dog-table "dog")
(def person-columns {:name 0 :age 1 :city 2 :country 3})
(def dog-columns {:name 0 :age 1 :city 2 :country 3 :owner 4})

(def person-query
  {:__result__
   [person-columns
    [["Ana" 80 "Athens" "Greece"]
     ["Charlie" 50 "Berlin" "Germany"]
     ["Alice" 30 "London" "UK"]
     ["David" 60 "Madrid" "Spain"]
     ["Bob" 40 "Paris" "France"]
     ["Eve" 70 "Rome" "Italy"]]]})

(def dog-query
  {:__result__
   [dog-columns
    [["Rover" 7 "Berlin" "Germany" "Charlie"]
     ["Fido" 3 "London" "UK" "Alice"]
     ["Spot" 5 "Madrid" "Spain" "David"]
     ["Rex" 3 "Paris" "France" "Bob"]
     ["Max" 6 "Rome" "Italy" "Eve"]
     ["Tok" 6 "Rome" "Italy" "Eve"]]]})

(def intermediate-result-set
  (assoc dog-query :people (:__result__ person-query)))

(def plan-nodes
  [:people [(csv-scan person-table)
            (projection :name :age :city)
            (selection [> :age "30"] and [< :age "70"])
            (sort :age)
            (limit 2)]
   :__result__ [(heap-file-scan dog-table)
                (sort :age :country)
                (projection :name :age :city)
                (selection [< :age "4"])
                (nested-loops-join [= :city :people/city] :people)
                (limit 2)]])

(deftest csv-scan-test
  (let [res ((csv-scan person-table) {})]
    (is (= {:__result__
            [{:name 0, :age 1, :city 2, :country 3}
             [["Alice" "30" "London" "UK"]
              ["Bob" "40" "Paris" "France"]
              ["Charlie" "50" "Berlin" "Germany"]
              ["David" "60" "Madrid" "Spain"]
              ["Eve" "70" "Rome" "Italy"]]]}
           (select-keys res [:__result__])))
    (.close ^Closeable (first (:__resources__ res)))))

(deftest heap-file-scan-test
  (let [res ((heap-file-scan dog-table) {})]
    (is (= {:__result__
            [{:name 0, :age 1, :city 2, :country 3, :owner 4}
             [["Fido" "3" "London" "UK" "Alice"]
              ["Rex" "3" "Paris" "France" "Bob"]
              ["Rover" "7" "Berlin" "Germany" "Charlie"]
              ["Spot" "5" "Madrid" "Spain" "David"]
              ["Max" "6" "Rome" "Italy" "Eve"]]]}
           (select-keys res [:__result__])))
    (.close ^Closeable (first (:__resources__ res)))))

(deftest projection-test
  (is (= {:__result__ [{:name 0 :age 1}
                       [["Ana" 80]
                        ["Charlie" 50]
                        ["Alice" 30]
                        ["David" 60]
                        ["Bob" 40]
                        ["Eve" 70]]]} ((projection :name :age) person-query))))

(deftest selection-test
  (is (= {:__result__ [{:name 0 :age 1 :city 2 :country 3}
                       [["Charlie" 50 "Berlin" "Germany"]
                        ["David" 60 "Madrid" "Spain"]
                        ["Bob" 40 "Paris" "France"]]]}
         ((selection [> :age 30] and [< :age 70]) person-query)))
  (is (= {:__result__ [{:name 0 :age 1 :city 2 :country 3}
                       [["Ana" 80 "Athens" "Greece"]
                        ["Charlie" 50 "Berlin" "Germany"]
                        ["Alice" 30 "London" "UK"]
                        ["David" 60 "Madrid" "Spain"]
                        ["Bob" 40 "Paris" "France"]
                        ["Eve" 70 "Rome" "Italy"]]]}
         ((selection [> :age 30] or [= :age 30]) person-query)))
  (is (= {:__result__ [{:name 0 :age 1 :city 2 :country 3}
                       [["Ana" 80 "Athens" "Greece"]
                        ["Charlie" 50 "Berlin" "Germany"]
                        ["David" 60 "Madrid" "Spain"]
                        ["Bob" 40 "Paris" "France"]
                        ["Eve" 70 "Rome" "Italy"]]]}
         ((selection [> :age 30]) person-query))))

(deftest limit-test
  (is (= {:__result__ [{:name 0 :age 1 :city 2 :country 3}
                       [["Ana" 80 "Athens" "Greece"]
                        ["Charlie" 50 "Berlin" "Germany"]]]}
         ((limit 2) person-query))))

(deftest sort-test
  (is (= {:__result__ [{:name 0 :age 1 :city 2 :country 3}
                       [["Alice" 30 "London" "UK"]
                        ["Bob" 40 "Paris" "France"]
                        ["Charlie" 50 "Berlin" "Germany"]
                        ["David" 60 "Madrid" "Spain"]
                        ["Eve" 70 "Rome" "Italy"]
                        ["Ana" 80 "Athens" "Greece"]]]}
         ((sort :age) person-query)))
  (is (= {:__result__ [{:name 0 :age 1 :city 2 :country 3 :owner 4}
                       [["Rex" 3 "Paris" "France" "Bob"]
                        ["Fido" 3 "London" "UK" "Alice"]
                        ["Spot" 5 "Madrid" "Spain" "David"]
                        ["Max" 6 "Rome" "Italy" "Eve"]
                        ["Tok" 6 "Rome" "Italy" "Eve"]
                        ["Rover"  7 "Berlin" "Germany" "Charlie"]]]}
         ((sort :age :country) dog-query))))

(deftest join-test
  (let [res {:__result__
             [{:name 0 :age 1 :city 2 :country 3 :owner 4 :people/name 5 :people/age 6 :people/city 7 :people/country 8}
              [["Rover" 7 "Berlin" "Germany" "Charlie" "Charlie" 50 "Berlin" "Germany"]
               ["Fido" 3 "London" "UK" "Alice" "Alice" 30 "London" "UK"]
               ["Spot" 5 "Madrid" "Spain" "David" "David" 60 "Madrid" "Spain"]
               ["Rex" 3 "Paris" "France" "Bob" "Bob" 40 "Paris" "France"]
               ["Max" 6 "Rome" "Italy" "Eve" "Eve" 70 "Rome" "Italy"]
               ["Tok" 6 "Rome" "Italy" "Eve" "Eve" 70 "Rome" "Italy"]]]}]
    (is (= res ((nested-loops-join
                 [= :city :people/city] :people) intermediate-result-set)))
    (is (= res ((hash-join
                 [= :city :people/city] :people) intermediate-result-set)))
    (is (= res ((sort-merge-join
                 [= :city :people/city] :people) intermediate-result-set)))))

(deftest execute-test
  (is (= [{:name "Rex" :age "3" :city "Paris" :people/age "40"
           :people/name "Bob" :people/city "Paris"}]
         (execute plan-nodes)))
  (is (= [{:name "Carlos" :a "a" :people1/name "Carlos" :b "b" :people2/name "Carlos"}]
         (execute [:people1 [(constantly {:__result__ [{:name 0 :a 1} [["Carlos" "a"]]]})]
                   :people2 [(constantly {:__result__ [{:b 0 :name 1} [["b" "Carlos"]]]})]
                   :__result__ [(constantly {:__result__ [{:name 0} [["Carlos"]]]})
                                (nested-loops-join [= :name :people1/name] :people1)
                                (nested-loops-join [= :name :people2/name] :people2)]]))))

(def movies-plan-nodes-csv
  [:__result__ [(csv-scan "movies")
                (sort :title)
                (limit 10)]])

(def ratings-plan-nodes-csv
  [:__result__ [(csv-scan "ratings")
                (limit 2)]])

(def tags-plan-nodes-csv
  [:__result__ [(csv-scan "tags")
                (sort :movieId)
                (limit 2)]])

(def movies-plan-nodes
  [:__result__ [(heap-file-scan "movies")
                (sort :title)
                (limit 10)]])

(def ratings-plan-nodes
  [:__result__ [(heap-file-scan "ratings")
                (sort :movieId)
                (limit 100)]])

(def tags-plan-nodes
  [:__result__ [(heap-file-scan "tags")
                (sort :movieId)
                (selection [= :userId "138472"])
                (limit 2)]])

(def ratings-by-movie-nested-loops
  [:movies [(heap-file-scan "movies")]
   :__result__ [(heap-file-scan "ratings")
                (nested-loops-join [= :movieId :movies/movieId] :movies)
                (limit 2)]])

(def ratings-by-movie-hash
  [:ratings [(heap-file-scan "ratings")]
   :__result__ [(heap-file-scan "movies")
                (hash-join [= :movieId :ratings/movieId] :ratings)
                (limit 2)]])

(def ratings-by-movie-sort-merge
  [:ratings [(heap-file-scan "ratings")
             (sort :movieId)]
   :__result__ [(heap-file-scan "movies")
                (hash-join [= :movieId :ratings/movieId] :ratings)
                (limit 2)]])

(comment
  (execute movies-plan-nodes-csv)
  (execute ratings-plan-nodes-csv)
  (time (execute tags-plan-nodes-csv))
  (execute movies-plan-nodes)
  (time (execute ratings-plan-nodes))
  (time (execute tags-plan-nodes))
  (time (execute ratings-by-movie-nested-loops))
  (time (execute ratings-by-movie-hash))
  (time (execute ratings-by-movie-sort-merge)))
