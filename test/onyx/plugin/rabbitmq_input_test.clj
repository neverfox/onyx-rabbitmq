(ns onyx.plugin.rabbitmq-input-test
  (:require
   [aero.core :refer [read-config]]
   [clojure.core.async :refer [>!!]]
   [clojure.test :refer [deftest is testing]]
   [hara.component :as component]
   [onyx.api]
   [onyx.job :refer [add-task]]
   [onyx.plugin.core-async :refer [get-core-async-channels take-segments!]]
   [onyx.plugin.rabbitmq-input]
   [onyx.rabbitmq :as rmq]
   [onyx.tasks.core-async :as core-async]
   [onyx.tasks.rabbitmq :refer [consumer serialize-message-edn]]
   [onyx.test-helper :refer [feedback-exception! with-test-env]]))

(defn build-job [params batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job       (merge {:workflow        [[:read-messages :identity]
                                                 [:identity :out]]
                               :catalog         [(merge {:onyx/name :identity
                                                         :onyx/fn   :clojure.core/identity
                                                         :onyx/type :function}
                                                        batch-settings)]
                               :lifecycles      []
                               :windows         []
                               :triggers        []
                               :flow-conditions []
                               :task-scheduler  :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (consumer :read-messages (merge params batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(defn produce-segments [producer n-messages]
  (let [ch (:write-ch producer)]
    (doseq [n (range n-messages)]
      (>!! ch {:n n}))
    (>!! ch :done)))

(deftest rabbitmq-input-test
  (let [test-queue                  (str "test.queue-" (java.util.UUID/randomUUID))
        {:keys [env-config
                peer-config
                rabbitmq-config]}   (read-config (clojure.java.io/resource "config.edn")
                                                 {:profile :test})
        tenancy-id                  (:onyx/tenancy-id env-config)
        env-config                  (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config                 (assoc peer-config :onyx/tenancy-id tenancy-id)
        uri                         (get-in rabbitmq-config [:rabbitmq/uri])
        params                      {:rabbitmq/queue           test-queue
                                     :rabbitmq/uri             uri
                                     :rabbitmq/deserializer-fn :onyx.tasks.rabbitmq/deserialize-message-edn}
        job                         (build-job params 10 1000)
        {:keys [out read-messages]} (get-core-async-channels job)
        producer                    (atom nil)
        n-messages                  15]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (reset! producer (component/start
                          (rmq/new-rabbit-producer {:params        {:queue test-queue :uri uri}
                                                    :serializer-fn serialize-message-edn})))
        (produce-segments @producer n-messages)
        (let [{:keys [job-id]} (onyx.api/submit-job peer-config job)
              expected         (set (map (fn [x] {:n x}) (range n-messages)))
              results          (take-segments! out 5000)]
          (feedback-exception! peer-config job-id)
          (is (= expected (set (butlast results))))
          (is (= :done (last results)))))
      (finally (swap! producer component/stop)))))
