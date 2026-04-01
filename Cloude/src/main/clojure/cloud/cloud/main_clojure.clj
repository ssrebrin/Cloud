(ns cloud.cloud.main-clojure
  (:require [cloud.cloud.client :as cloud]))

(cloud/defcloudfn square [x]
  (* x x))

(cloud/defcloudfn even-remote? [x]
  (zero? (mod x 2)))

(cloud/defcloudfn times10 [x]
  (* x 10))

(cloud/defcloudfn sum2 [a b]
  (+ a b))

(defn -main [& _]
  (let [legacy-client (cloud/connect "http://localhost:8085")
        legacy-result (cloud/execute-annotated legacy-client #'square [2 3 4 5])
        stream-result (-> (cloud/stream-connect "http://localhost:8085")
                          (cloud/stream [1 2 3 4 5 6])
                          (cloud/filter-annotated #'even-remote?)
                          (cloud/map-annotated #'times10)
                          (cloud/reduce-annotated #'sum2)
                          (cloud/execute-stream))]
    (println "[clojure][legacy] squares:" legacy-result)
    (println "[clojure][pipeline] even*10 sum:" stream-result)))
