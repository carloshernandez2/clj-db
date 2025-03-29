(ns query-executor-test
  (:refer-clojure :exclude [and or sort < >])
  (:require
   [clojure.test :refer [deftest is]]
   [query-executor :refer [< > and csv-scan execute heap-file-scan limit 
                           nested-loops-join or projection selection sort]]))

(def person-table "person")
(def dog-table "dog")

(def person-query
  {:__result__
   [{:name "Alice" :age 30 :city "London" :country "UK"}
    {:name "Bob" :age 40 :city "Paris" :country "France"}
    {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
    {:name "David" :age 60 :city "Madrid" :country "Spain"}
    {:name "Eve" :age 70 :city "Rome" :country "Italy"}]})

(def dog-query
  {:__result__
   [{:name "Fido" :age 3 :city "London" :country "UK" :owner "Alice"}
    {:name "Rex" :age 3 :city "Paris" :country "France" :owner "Bob"}
    {:name "Rover" :age 7 :city "Berlin" :country "Germany" :owner "Charlie"}
    {:name "Spot" :age 5 :city "Madrid" :country "Spain" :owner "David"}
    {:name "Max" :age 6 :city "Rome" :country "Italy" :owner "Eve"}]})

(def intermediate-result-set
  (assoc dog-query :people (:__result__ person-query)))

(def plan-nodes
  [:people [(csv-scan person-table)
            (projection :name :age :city)
            (selection [> :age "30"] and [< :age "70"])
            (limit 2)
            (sort :age)]
   :__result__ [(heap-file-scan dog-table)
                (sort :age :country)
                (projection :name :age :city)
                (selection [< :age "4"])
                (nested-loops-join [= :city :people/city] :people)
                (limit 2)]])

(deftest csv-scan-test
  (let [res ((csv-scan person-table) person-query)]
    (is (= {:__result__
            [{:name "Alice", :age "30", :city "London", :country "UK"}
             {:name "Bob", :age "40", :city "Paris", :country "France"}
             {:name "Charlie", :age "50", :city "Berlin", :country "Germany"}
             {:name "David", :age "60", :city "Madrid", :country "Spain"}
             {:name "Eve", :age "70", :city "Rome", :country "Italy"}]}
           (select-keys res [:__result__])))
    (.close (first (:__resources__ res)))))

(deftest heap-file-scan-test
  (let [res ((heap-file-scan person-table) person-query)]
    (is (= {:__result__
            [{:name "Alice", :age "30", :city "London", :country "UK"}
             {:name "Bob", :age "40", :city "Paris", :country "France"}
             {:name "Charlie", :age "50", :city "Berlin", :country "Germany"}
             {:name "David", :age "60", :city "Madrid", :country "Spain"}
             {:name "Eve", :age "70", :city "Rome", :country "Italy"}]}
           (select-keys res [:__result__])))
    (.close (first (:__resources__ res)))))

(deftest projection-test
  (is (= {:__result__ [{:name "Alice" :age 30}
                       {:name "Bob" :age 40}
                       {:name "Charlie" :age 50}
                       {:name "David" :age 60}
                       {:name "Eve" :age 70}]} ((projection :name :age) person-query))))

(deftest selection-test
  (is (= {:__result__ [{:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}]}
         ((selection [> :age 30] and [< :age 70]) person-query)))
  (is (= {:__result__ [{:name "Alice" :age 30 :city "London" :country "UK"}
                       {:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}
                       {:name "Eve" :age 70 :city "Rome" :country "Italy"}]}
         ((selection [> :age 30] or [= :age 30]) person-query)))
  (is (= {:__result__ [{:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}
                       {:name "Eve" :age 70 :city "Rome" :country "Italy"}]}
         ((selection [> :age 30]) person-query))))

(deftest limit-test
  (is (= {:__result__ [{:name "Alice" :age 30 :city "London" :country "UK"}
                       {:name "Bob" :age 40 :city "Paris" :country "France"}]}
         ((limit 2) person-query))))

(deftest sort-test
  (is (= {:__result__ [{:name "Alice" :age 30 :city "London" :country "UK"}
                       {:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}
                       {:name "Eve" :age 70 :city "Rome" :country "Italy"}]}
         ((sort :age) person-query)))
  (is (= {:__result__ [{:name "Rex" :age 3 :city "Paris" :country "France" :owner "Bob"}
                       {:name "Fido" :age 3 :city "London" :country "UK" :owner "Alice"}
                       {:name "Spot" :age 5 :city "Madrid" :country "Spain" :owner "David"}
                       {:name "Max" :age 6 :city "Rome" :country "Italy" :owner "Eve"}
                       {:name "Rover" :age 7 :city "Berlin" :country "Germany" :owner "Charlie"}]}
         ((sort :age :country) dog-query))))

(deftest nested-loops-join-test
  (is (= {:__result__
          [{:age 3 :name "Fido" :city "London" :people/country "UK" :people/age 30
            :people/city "London" :people/name "Alice" :country "UK" :owner "Alice"}
           {:age 3 :name "Rex" :city "Paris" :people/country "France" :people/age 40
            :people/city "Paris" :people/name "Bob" :country "France" :owner "Bob"}
           {:age 7 :name "Rover" :city "Berlin" :people/country "Germany" :people/age 50
            :people/city "Berlin" :people/name "Charlie" :country "Germany" :owner "Charlie"}
           {:age 5 :name "Spot" :city "Madrid" :people/country "Spain" :people/age 60
            :people/city "Madrid" :people/name "David" :country "Spain" :owner "David"}
           {:age 6 :name "Max" :city "Rome" :people/country "Italy" :people/age 70
            :people/city "Rome" :people/name "Eve" :country "Italy" :owner "Eve"}]}
         ((nested-loops-join
           [= :city :people/city] :people) intermediate-result-set))))

(deftest execute-test
  (is (= [{:name "Rex" :age "3" :city "Paris" :people/age "40"
           :people/name "Bob" :people/city "Paris"}]
         (execute plan-nodes))))

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
                #_(selection [= :userId "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"])
                (limit 2)]])

(def tags-plan-nodes
  [:__result__ [(heap-file-scan "tags")
                (sort :movieId)
                #_(selection [= :userId "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"])
                (limit 2)]])

(def ratings-by-movie
  [:movies [(heap-file-scan "movies")]
   :__result__ [(heap-file-scan "ratings")
                (nested-loops-join [= :movieId :movies/movieId] :movies)
                (limit 2)]])

(comment
  (execute movies-plan-nodes-csv)
  (execute ratings-plan-nodes-csv)
  (time (execute tags-plan-nodes-csv))
  (execute movies-plan-nodes)
  (execute ratings-plan-nodes)
  (time (execute tags-plan-nodes))
  (time (execute ratings-by-movie)))

