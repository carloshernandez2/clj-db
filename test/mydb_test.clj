(ns mydb-test
  (:refer-clojure :exclude [and or sort < >])
  (:require
   [clojure.test :refer [deftest is]]
   [mydb :refer [and limit merge or projection scan selection sort < >]]))

(def person-table "person_table.csv")
(def dog-table "dog_table.csv")

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
  [:people [(scan person-table)
            (projection :name :age)
            (selection [> :age "30"] and [< :age "70"])
            (limit 2)
            (sort :age)]
   :__result__ [(scan dog-table)
                (sort :age :country)
                (projection :name :age)
                (selection [< :age "4"])
                (limit 2)
                (merge :people)]])

(deftest scan-test
  (let [res ((mydb/scan person-table) person-query)]
    (is (= {:__result__
            [{:name "Alice", :age "30", :city "London", :country "UK"}
             {:name "Bob", :age "40", :city "Paris", :country "France"}
             {:name "Charlie", :age "50", :city "Berlin", :country "Germany"}
             {:name "David", :age "60", :city "Madrid", :country "Spain"}
             {:name "Eve", :age "70", :city "Rome", :country "Italy"}]}
           (select-keys res [:__result__])))
    (.close (first (:__resourses__ res)))))

(deftest projection-test
  (is (= {:__result__ [{:name "Alice" :age 30}
                       {:name "Bob" :age 40}
                       {:name "Charlie" :age 50}
                       {:name "David" :age 60}
                       {:name "Eve" :age 70}]} ((mydb/projection :name :age) person-query))))

(deftest selection-test
  (is (= {:__result__ [{:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}]}
         ((mydb/selection [> :age 30] and [< :age 70]) person-query)))
  (is (= {:__result__ [{:name "Alice" :age 30 :city "London" :country "UK"}
                       {:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}
                       {:name "Eve" :age 70 :city "Rome" :country "Italy"}]}
         ((mydb/selection [> :age 30] or [= :age 30]) person-query)))
  (is (= {:__result__ [{:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}
                       {:name "Eve" :age 70 :city "Rome" :country "Italy"}]}
         ((mydb/selection [> :age 30]) person-query))))

(deftest limit-test
  (is (= {:__result__ [{:name "Alice" :age 30 :city "London" :country "UK"}
                       {:name "Bob" :age 40 :city "Paris" :country "France"}]}
         ((mydb/limit 2) person-query))))

(deftest sort-test
  (is (= {:__result__ [{:name "Alice" :age 30 :city "London" :country "UK"}
                       {:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}
                       {:name "Eve" :age 70 :city "Rome" :country "Italy"}]}
         ((mydb/sort :age) person-query)))
  (is (= {:__result__ [{:name "Rex" :age 3 :city "Paris" :country "France" :owner "Bob"}
                       {:name "Fido" :age 3 :city "London" :country "UK" :owner "Alice"}
                       {:name "Spot" :age 5 :city "Madrid" :country "Spain" :owner "David"}
                       {:name "Max" :age 6 :city "Rome" :country "Italy" :owner "Eve"}
                       {:name "Rover" :age 7 :city "Berlin" :country "Germany" :owner "Charlie"}]}
         ((mydb/sort :age :country) dog-query))))

(deftest merge-test
  (is (= {:__result__ [{:name "Fido" :age 3 :city "London" :country "UK" :owner "Alice"}
                       {:name "Rex" :age 3 :city "Paris" :country "France" :owner "Bob"}
                       {:name "Rover" :age 7 :city "Berlin" :country "Germany" :owner "Charlie"}
                       {:name "Spot" :age 5 :city "Madrid" :country "Spain" :owner "David"}
                       {:name "Max" :age 6 :city "Rome" :country "Italy" :owner "Eve"}
                       {:name "Alice" :age 30 :city "London" :country "UK"}
                       {:name "Bob" :age 40 :city "Paris" :country "France"}
                       {:name "Charlie" :age 50 :city "Berlin" :country "Germany"}
                       {:name "David" :age 60 :city "Madrid" :country "Spain"}
                       {:name "Eve" :age 70 :city "Rome" :country "Italy"}]}
         ((mydb/merge :people) intermediate-result-set))))

(deftest execute-test
  (is (= [{:name "Rex", :age "3"}
          {:name "Fido", :age "3"}
          {:name "Bob", :age "40"}
          {:name "Charlie", :age "50"}]
         (mydb/execute plan-nodes))))

(def movies-plan-nodes
  [:__result__ [(scan "movies.csv")
                (sort :title)
                (limit 10)]])

(def ratings-plan-nodes
  [:__result__ [(scan "ratings.csv")
                (limit 2)]])

(def tags-plan-nodes
  [:__result__ [(scan "tags.csv")
                (sort :movieId)
                (limit 2)]])

(comment
  (mydb/execute movies-plan-nodes)
  (mydb/execute ratings-plan-nodes)
  (mydb/execute tags-plan-nodes))
