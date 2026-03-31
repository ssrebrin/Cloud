(ns cloud.cloud.main-clojure
  (:require [cloud.cloud.client :as cloud]))

(defn -main [& _]
  (let [legacy-client (cloud/connect "http://localhost:8085")
        legacy-result (cloud/execute-code legacy-client (fn [x] (* x x)) [2 3 4 5])
        stream-result (-> (cloud/stream-connect "http://localhost:8085")
                          (cloud/stream [1 2 3 4 5 6])
                          (cloud/filter-code (fn [x] (zero? (mod x 2))))
                          (cloud/map-code (fn [x] (* x 10)))
                          (cloud/reduce-code (fn [a b] (+ a b)))
                          (cloud/execute-stream))]
    (println "[clojure][legacy] squares:" legacy-result)
    (println "[clojure][pipeline] even*10 sum:" stream-result)))
