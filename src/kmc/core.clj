(ns kmc.core
  "Kafka Metrics (OpenTSDB) Consumer"
  (:gen-class))

(require '[clojure.core.async :refer [>!! close!]])
(require 'kmc.influxdb_consumer)
(require '[clojure.tools.cli :refer [parse-opts]])

(defn setup-consumer [options]
  (let [consumer-type (:target options)
        params (:url options)
        limit (:limit options)
        consumer-fn (resolve
                     (symbol
                      (str "kmc." (name consumer-type) "_consumer"
                           "/make-consumer")))]

    (consumer-fn params limit)))

(def cli-options
  [["-t" "--target destination" "Where to send metrics to (opentsdb or influxdb)"
    :default "opentsdb"
    :parse-fn #(keyword (.toLowerCase (str %)))
    :validate [#(.contains [:opentsdb :influxdb] %) "Must be one of 'influxdb' or 'opentsdb'"]]
   ["-u" "--url url" "The connection string (url) to send metrics to"
    :default "localhost:4242"
    :parse-fn #(str %)
    :validate [#(string? %) "Connection string to send metrics to."]]
   ["-l" "--limit limit" "Limits the queue size of metrics waiting to be sent to the consumers."
    :default 50000
    :parse-fn #(read-string %)
    :validate [#(pos? %) "A positive number that limits the queue size."]]
   ["-h" "--help"]])

(defn -main [& args]
  (let [parsed-args (parse-opts args cli-options)
        options (:options parsed-args)]
    (when (or (:help options) (:errors parsed-args))
      (println (:summary (parse-opts args cli-options)))
      (System/exit 0))
    (let [consumer-ch (setup-consumer options)
          incoming (line-seq (java.io.BufferedReader. *in*))]
      (doseq [line incoming]
        (>!! consumer-ch line))
      (System/exit 0))))
