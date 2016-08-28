(defproject onyx-rabbitmq "0.9.9.0"
  :description "Onyx plugin for rabbitmq"
  :url "FIX ME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.onyxplatform/onyx "0.9.9"]
                 [com.novemberain/langohr "3.6.1"]
                 [less-awful-ssl "1.0.1"]
                 [environ "1.1.0"]]
  :plugins [[lein-environ "1.0.0"]]
  :profiles {:dev {:dependencies []
                   :plugins      []}})
