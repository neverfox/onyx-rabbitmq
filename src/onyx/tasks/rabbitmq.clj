(ns onyx.tasks.rabbitmq
  (:require
   [cheshire.core :as json]
   [cognitect.transit :as transit]
   [onyx.job :refer [add-task]]
   [onyx.schema :as os]
   [schema.core :as s])
  (:import
   (java.io ByteArrayInputStream ByteArrayOutputStream)))

;;;; Reader task
(defn deserialize-message-edn [bytes]
  (try
    (read-string (String. bytes "UTF-8"))
    (catch Exception e
      {:error e})))

(defn deserialize-message-json [bytes]
  (try
    (json/parse-string (String. bytes "UTF-8"))
    (catch Exception e
      {:error e})))

(defn deserialize-message-transit-json [bytes]
  (try
    (let [in     (ByteArrayInputStream. bytes)
          reader (transit/reader in :json)]
      (transit/read reader))
    (catch Exception e
      {:error e})))

(def RabbitInputTaskMap
  {:rabbitmq/queue                        s/Str
   :rabbitmq/uri                          s/Str
   :rabbitmq/deserializer-fn              os/NamespacedKeyword
   (s/optional-key :rabbitmq/key)         s/Str
   (s/optional-key :rabbitmq/crt)         s/Str
   (s/optional-key :rabbitmq/ca-crt)      s/Str
   (s/optional-key :rabbitmq/durable)     s/Bool
   (s/optional-key :rabbitmq/exclusive)   s/Bool
   (s/optional-key :rabbitmq/auto-delete) s/Bool
   (os/restricted-ns :rabbitmq)           s/Any})

(s/defn ^:always-validate consumer
  ([task-name :- s/Keyword opts]
   {:task   {:task-map   (merge {:onyx/name   task-name
                                 :onyx/plugin :onyx.plugin.rabbitmq-input/input
                                 :onyx/type   :input
                                 :onyx/medium :rabbitmq
                                 :onyx/doc    "Reads messages from a RabbitMQ queue"}
                                opts)
             :lifecycles [{:lifecycle/task  task-name
                           :lifecycle/calls :onyx.plugin.rabbitmq-input/reader-calls}]}
    :schema {:task-map RabbitInputTaskMap}})
  ([task-name :- s/Keyword
    queue :- s/Str
    uri :- s/Str
    deserializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (consumer task-name (merge {:rabbitmq/queue           queue
                               :rabbitmq/uri             uri
                               :rabbitmq/deserializer-fn deserializer-fn}
                              task-opts))))

;;;; Writer task
(defn serialize-message-edn [segment]
  (.getBytes (pr-str segment)))

(defn serialize-message-json [segment]
  (.getBytes (json/generate-string segment)))

(defn serialize-message-transit-json [segment]
  (let [out    (ByteArrayOutputStream.)
        writer (transit/writer out :json)]
    (transit/write writer segment)
    (.toByteArray out)))

(def RabbitOutputTaskMap
  {(s/optional-key :rabbitmq/queue)       s/Str
   :rabbitmq/uri                          s/Str
   :rabbitmq/serializer-fn                os/NamespacedKeyword
   (s/optional-key :rabbitmq/durable)     s/Bool
   (s/optional-key :rabbitmq/exclusive)   s/Bool
   (s/optional-key :rabbitmq/auto-delete) s/Bool
   (os/restricted-ns :rabbitmq)           s/Any})

(s/defn ^:always-validate producer
  ([task-name :- s/Keyword opts]
   {:task   {:task-map   (merge {:onyx/name   task-name
                                 :onyx/plugin :onyx.plugin.rabbitmq-output/write-messages
                                 :onyx/type   :output
                                 :onyx/medium :rabbitmq
                                 :onyx/doc    "Writes messages to a RabbitMQ queue"}
                                opts)
             :lifecycles [{:lifecycle/task  task-name
                           :lifecycle/calls :onyx.plugin.rabbitmq-output/writer-calls}]}
    :schema {:task-map RabbitOutputTaskMap}})
  ([task-name :- s/Keyword
    queue :- s/Str
    uri :- s/Str
    serializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (producer task-name (merge {:rabbitmq/queue         queue
                               :rabbitmq/uri           uri
                               :rabbitmq/serializer-fn serializer-fn}
                              task-opts))))
