(ns cloud.cloud.client
  (:import (cloud.domain Operation Task)
           (com.fasterxml.jackson.databind ObjectMapper)
           (java.net URI)
           (java.net.http HttpClient HttpRequest HttpResponse HttpRequest$BodyPublishers HttpResponse$BodyHandlers)
           (java.nio.charset StandardCharsets)))

(def ^:private poll-ms 500)

(defn connect [url]
  {:manager-url url
   :client (HttpClient/newHttpClient)
   :mapper (ObjectMapper.)})

(defn stream-connect [url]
  {:manager-url url
   :client (HttpClient/newHttpClient)
   :mapper (ObjectMapper.)
   :values nil
   :ops []})

(defn stream [ctx values]
  (assoc ctx :values (vec values)))

(defn- ->source [x]
  (if (string? x) x (pr-str x)))

(defn map-op [ctx clojure-fn]
  (update ctx :ops conj
          (Operation. "map"
                      (.getBytes (->source clojure-fn) StandardCharsets/UTF_8)
                      Task/LANGUAGE_CLOJURE)))

(defn filter-op [ctx clojure-predicate]
  (update ctx :ops conj
          (Operation. "filter"
                      (.getBytes (->source clojure-predicate) StandardCharsets/UTF_8)
                      Task/LANGUAGE_CLOJURE)))

(defn reduce-op [ctx clojure-reducer]
  (update ctx :ops conj
          (Operation. "reduce"
                      (.getBytes (->source clojure-reducer) StandardCharsets/UTF_8)
                      Task/LANGUAGE_CLOJURE)))

(defmacro map-code [ctx fn-form]
  `(map-op ~ctx ~(pr-str fn-form)))

(defmacro filter-code [ctx fn-form]
  `(filter-op ~ctx ~(pr-str fn-form)))

(defmacro reduce-code [ctx fn-form]
  `(reduce-op ~ctx ~(pr-str fn-form)))

(defn- parse-int-list [raw]
  (cond
    (nil? raw) []
    (instance? java.util.List raw) (mapv #(int (long %)) raw)
    :else []))

(defn- parse-any-list [raw]
  (cond
    (nil? raw) []
    (instance? java.util.List raw) (vec raw)
    :else [raw]))

(defn- submit-request [ctx body]
  (let [request (-> (HttpRequest/newBuilder)
                    (.uri (URI/create (str (:manager-url ctx) "/execute")))
                    (.header "Content-Type" "application/json")
                    (.POST (HttpRequest$BodyPublishers/ofString
                            (.writeValueAsString (:mapper ctx) body)))
                    (.build))
        response (.send (:client ctx) request (HttpResponse$BodyHandlers/ofString))
        response-map (.readValue (:mapper ctx) (.body response) java.util.Map)]
    (when-not (= "accepted" (.get response-map "status"))
      (throw (RuntimeException. "Task was not accepted")))
    (str (.get response-map "taskId"))))

(defn- wait-for-result [ctx task-id result-parser]
  (loop []
    (let [request (-> (HttpRequest/newBuilder)
                      (.uri (URI/create (str (:manager-url ctx) "/result/" task-id)))
                      (.GET)
                      (.build))
          response (.send (:client ctx) request (HttpResponse$BodyHandlers/ofString))
          result-map (.readValue (:mapper ctx) (.body response) java.util.Map)
          status (str (.get result-map "status"))]
      (cond
        (= "done" status) (result-parser (.get result-map "result"))
        (= "error" status) (throw (RuntimeException. (str (.get result-map "error"))))
        :else (do (Thread/sleep poll-ms)
                  (recur))))))

(defn execute [ctx clojure-fn values]
  (let [payload-values (mapv int values)
        task-id (submit-request ctx {"serializedFunction" (->source clojure-fn)
                                     "values" payload-values
                                     "callback" "http://localhost:8087/callback"
                                     "language" Task/LANGUAGE_CLOJURE})]
    (wait-for-result ctx task-id parse-int-list)))

(defmacro execute-code [ctx fn-form values]
  `(execute ~ctx ~(pr-str fn-form) ~values))

(defn execute-stream [ctx]
  (when (nil? (:values ctx))
    (throw (IllegalStateException. "No input data. Call stream before execute-stream.")))
  (let [task-id (submit-request ctx {"ops" (:ops ctx)
                                     "data" (:values ctx)
                                     "values" (:values ctx)
                                     "callback" "http://localhost:8087/callback"
                                     "language" Task/LANGUAGE_CLOJURE})]
    (wait-for-result ctx task-id parse-any-list)))
