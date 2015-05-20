(ns kmc.core
  "OpenTSDB Metrics Consumer"
  (:gen-class))

(require '[clojure.core.async :refer [>!! close! buffer]])
(require '[clojure.tools.logging :as log])
(require 'kmc.influxdb_consumer)
(require '[clojure.tools.cli :refer [parse-opts]])

(require '[clojure.core.async.impl.concurrent :as conc])
(require '[clojure.core.async.impl.exec.threadpool :as tp])

(defonce my-executor
  (java.util.concurrent.Executors/newFixedThreadPool
   1
   (conc/counted-thread-factory "kmc-async-dispatch-%d" true)))

(alter-var-root #'clojure.core.async.impl.dispatch/executor
                (constantly (delay (tp/thread-pool-executor my-executor))))

(defn setup-consumer [options]
  (let [consumer-type (:target options)
        params (:url options)
        buf (buffer (:limit options))
        consumer-fn (resolve
                     (symbol
                      (str "kmc." (name consumer-type) "_consumer"
                           "/make-consumer")))]

    (consumer-fn params buf)))

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
      (println (:summary parsed-args))
      (System/exit 0))
    (let [[consumer-ref consumer-ch] (setup-consumer options)
          incoming (line-seq (java.io.BufferedReader. *in*))]
      (doseq [line incoming]
        (>!! consumer-ch line))
      (close! consumer-ch)
      (log/warn "Input stream closed, waiting for consumer thread to finish.")
      @consumer-ref
      (log/info "Consumer thread exited, shutting down.")
      (System/exit 0))))
