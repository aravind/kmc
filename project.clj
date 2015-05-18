(defproject kmc "0.1.1"
  :description "Pushes OpenTSDB metrics from stdin to multiple consumers"
  :url "http://opentsdb.net"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :main kmc.core
  :aot [kmc.core]
  :jvm-opts ["-verbosegc" "-Xmx256M" "-Xms256M"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/data.json "0.2.6"]
                 [ch.qos.logback/logback-classic "1.0.9"]
                 [http-kit "2.1.18"]])
