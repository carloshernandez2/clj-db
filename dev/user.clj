(ns user
  #_{:clj-kondo/ignore [:unused-namespace]}
  #_{:clj-kondo/ignore [:unused-referred-var]}
  (:require [clojure.tools.namespace.repl :refer [refresh]]))

(def total-time (volatile! 0))

(defmacro timed [f]
  `(let [start# (System/nanoTime)
         result# ~f
         end# (System/nanoTime)]
     (vswap! total-time + (- end# start#))
     result#))

(defn reset-timer []
  (vreset! total-time 0))

(defn get-total-time []
  (/ @total-time 1e6))

