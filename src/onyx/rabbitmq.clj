(ns onyx.rabbitmq
  (:require
   [clojure.core.async :refer [<! >!! chan close! go-loop]]
   [hara.component :as component :refer [IComponent]]
   [langohr.basic :as lb]
   [langohr.channel :as lch]
   [langohr.consumers :as lc]
   [langohr.core :as rmq]
   [langohr.queue :as lq]
   [less.awful.ssl :as awful]
   [taoensso.timbre :as timbre :refer [info]])
  (:import
   (java.io FileNotFoundException)
   (java.security.cert CertificateException)))

(defn create-ssl-context
  "Returns an SSL context for use when initiating an SSL connection.
   Parameters are absolute paths to SSL credentials."

  [client-key client-crt ca-crt]
  (try
    (awful/ssl-context client-key client-crt ca-crt)

    (catch FileNotFoundException e
      (let [file (clojure.string/replace (.getMessage e) #" \(No such file or directory\)" "")]
        (throw (ex-info "Failed to create an SSL context (file not found)" {:file file}))))
    (catch NullPointerException e
      (throw (ex-info "Failed to create an SSL context (null pointer. maybe an invalid ssl key)"
                      {:key client-key})))
    (catch CertificateException e
      (throw (ex-info "Failed to create an SSL context (invalid ssl certificate)"
                      {:crt client-crt :ca-crt ca-crt})))))

(defn params->config
  [params]
  (dissoc (if (not-any? nil? (map params [:key :crt :ca-crt]))
            (assoc params
                   :ssl true
                   :authentication-mechanism "EXTERNAL"
                   :ssl-context (apply create-ssl-context (map params [:key :crt :ca-crt])))
            (dissoc params :key :crt :ca-crt))
          :queue :key :crt :ca-crt :exclusive :durable :auto-delete))

(defn message-handler
  [write-to-chan deserialize-fn]
  (fn
    [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
    (>!! write-to-chan {:payload (deserialize-fn payload) :delivery-tag delivery-tag})))

(defprotocol IRabbitConsumer
  (ack [this delivery-tag])
  (requeue [this delivery-tag]))

(defrecord RabbitConsumer []
  Object
  (toString [self]
    (str "#rabbit-consumer" (into {} self)))

  IComponent
  (-start [{:keys [params deserializer-fn write-to-ch] :as self}]
    (info "-> Starting RabbitMQ consumer")
    (let [conn    (rmq/connect (params->config params))
          ch      (lch/open conn)
          queue   (:queue params)
          options (select-keys params [:durable :exclusive :auto-delete])]
      (lq/declare ch queue options)
      (lc/subscribe ch queue (message-handler write-to-ch deserializer-fn) {:auto-ack false})
      (assoc self :conn conn :ch ch)))
  (-stop [{:keys [ch conn] :as self}]
    (info "<- Stopping RabbitMQ consumer")
    (rmq/close ch)
    (rmq/close conn)
    (dissoc self :ch :conn))

  IRabbitConsumer
  (ack [{:keys [ch]} delivery-tag]
    (lb/ack ch delivery-tag))
  (requeue [{:keys [ch]} delivery-tag]
    (lb/reject ch delivery-tag true)))

(defmethod print-method RabbitConsumer
  [v w]
  (.write w (str v)))

(defn new-rabbit-consumer [m] (map->RabbitConsumer m))

(defrecord RabbitProducer []
  Object
  (toString [self]
    (str "#rabbit-producer" (into {} self)))

  IComponent
  (-start [{:keys [params serializer-fn] :as self}]
    (info "-> Starting RabbitMQ producer")
    (let [write-ch (chan 2)
          conn     (rmq/connect (params->config params))
          ch       (lch/open conn)
          queue    (:queue params)
          options  (select-keys params [:durable :exclusive :auto-delete])]
      (lq/declare ch queue options)
      (go-loop []
        (let [msg (<! write-ch)]
          (info "Writing" msg "to" queue)
          (lb/publish ch "" queue (serializer-fn msg))
          (recur)))
      (assoc self :conn conn :ch ch :write-ch write-ch)))
  (-stop [{:keys [ch conn write-ch] :as self}]
    (info "<- Stopping RabbitMQ producer")
    (close! write-ch)
    (rmq/close ch)
    (rmq/close conn)
    (dissoc self :ch :conn :write-ch)))

(defmethod print-method RabbitProducer
  [v w]
  (.write w (str v)))

(defn new-rabbit-producer [m] (map->RabbitProducer m))
