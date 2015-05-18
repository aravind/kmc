(ns kmc.influxdb_consumer)

(require '[clojure.core.async :as async])
(require '[clojure.tools.logging :as log])
(require '[clojure.data.json :refer [write-str]])
(require '[org.httpkit.client :as http])

(defn number->string [^String s]
  (try
    (let [n (read-string s)]
      (if (number? n) n s))
    (catch RuntimeException e
      s)))

(defn string-to-tv-tupples [^String s]
  (clojure.string/split s #"="))

(defn combiner [[ts vs] [t v]]
  [(conj ts t) (conj vs v)])

(defn extract-tags-and-values [t-v-strings]
  "Takes ['k1=v1' 'k2=v2' 'k3=v3'] and returns [['k1' 'k2' 'k3'] ['v1' 'v2' 'v3]]"
  (reduce combiner [[] []] (map string-to-tv-tupples (sort t-v-strings))))

(defn make-influxdb-metric [^String tcollector-metric-line]
  "converts 'proc.loadavg 1430641159 0.2 type=1m host=foo' into a
   hash that looks like {'proc.loadavg' {'columns' ('time' 'value'
   'host' 'type'), 'points' [(1430641159, 0.2, 'foo', '1m')]}}"
  (let [metric-string-parts (clojure.string/split tcollector-metric-line #"\s+")]
    (if (>= (count metric-string-parts) 3)
      (let [[metric-name ts value & t-v-strings] metric-string-parts
            [tags values] (extract-tags-and-values t-v-strings)]
        {metric-name {"columns" (into ["time" "value"] tags)
                      "points" [(map number->string (into [ts value] values))]}})
      (log/warn "Invalid metric:" tcollector-metric-line))))

(defn merge-points [m1 m2]
  {"columns" (get m1 "columns")
   "points" (into (get m1 "points") (get m2 "points"))})

(defn aggregate-metrics [influxdb-metrics]
  "influxdb-metrics is a seq of hashes that look like {'metric-name'
   {'columns' (c1 c2 ..), 'points' [(p1 p2 ..)]}}.  We aggregate the
   influxdb-metrics based on the metric name.  The hash is then
   transformed into a seq that looks like ({'name' 'metric-name',
   'columns' (c1 c2 ..), 'points' ((p11 p12 ..) (p21 p22 ..))} ..)"
  (let [grouped-metrics (apply merge-with merge-points influxdb-metrics)]
    (map
     (fn [[k v]] (into {"name" k} v))
     grouped-metrics)))

(defn post-datapoints-to-influxdb [^String url ^String json-body]
  (try
    (let [response @(http/post url {:body json-body})]
      (= 200 (:status response)))
    (catch Exception ex
      (log/warn "Got exception:" ex)
      false)))

(defn send-to-influxdb [num-points queue-ch ^String url]
  (let [metric-points (async/<!!
                       (async/into [] (async/take num-points queue-ch)))
        influxdb-data-points (aggregate-metrics
                              (map make-influxdb-metric metric-points))
        json-body (write-str influxdb-data-points)]
    (loop [attempt-num 1]
      (log/info "Attempt:" attempt-num "sending" num-points "metrics to Influxdb.")
      (if (post-datapoints-to-influxdb url json-body)
        (log/info "Added" num-points "metrics to Influxdb.")
        (do
          (log/warn "Failed to add metrics to Influxdb, re-trying in 1s")
          (Thread/sleep 1000)
          (recur (inc attempt-num)))))))

(defn make-consumer [^String url limit]
  "This consumer lets metrics queue up for 2s, and then sends them to
   influxdb as a batch (makes it more efficient for influxdb)."
  (let [buf (async/buffer limit)
        queue (async/chan buf)
        batch-interval 2000]
    (future
      (loop [wait-millisecs batch-interval]
        (Thread/sleep wait-millisecs)
        (let [num-points (count buf)]
          (if (= num-points 0)
            (log/info "Queue empty, nothing to send to Influxdb")
            (send-to-influxdb num-points queue url)))
        (recur batch-interval)))
    queue))

