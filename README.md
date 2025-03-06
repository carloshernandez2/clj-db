# clj-db

So far... 

- A query executor that takes a parsed query plan made up of plan nodes assigned to query keys and executes it retrieving data from csv files and a custom heap file format (check the drawio diagram for details)
- A module that converts CSV files to the custom heap file format

It makes use of lazy sequences as an interface between query plan nodes to avoid fetching all the data when it is not necessary.

This behavior was confirmed using the movilens dataset, which contains files in the order of MiBs. You can download it [here](https://grouplens.org/datasets/movielens/20m/).

Start a REPL, download the dataset and check the clojure.test files to play with the code. If you are new to clojure you can get started [here](https://clojure.org/guides/getting_started) 
