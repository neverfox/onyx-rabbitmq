(defproject onyx-rabbitmq "0.9.9.0"
  :description "Onyx plugin for RabbitMQ"
  :url "https://github.com/neverfox/onyx-rabbitmq"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[aero "1.0.0"]
                 [cheshire "5.6.3"]
                 [com.cognitect/transit-clj "0.8.288"]
                 [com.novemberain/langohr "3.6.1"]
                 [im.chit/hara.component "2.3.6"]
                 [less-awful-ssl "1.0.1"]
                 [org.clojure/clojure "1.8.0"]
                 [org.onyxplatform/onyx "0.9.9"]
                 [prismatic/schema "1.1.3"]]
  :plugins [[lein-environ "1.0.0"]]
  :profiles {:dev {:plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}})
