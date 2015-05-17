# kmc

A Clojure program that pulls Statsd and OpenTSDB metrics from a topic
in Kafka and pushes them to multiple downstream consumers.

## Notes

The consumer takes the folling options.

  -t, --target destination  :opentsdb       Where to send metrics to (opentsdb or influxdb)
  -u, --url url             localhost:4242  The connection string (url) to send metrics to
  -l, --limit limit         50000           Limits the queue size of metrics waiting to be sent to the consumers.
  -h, --help

## Usage

lein run (or lein trampoline run) or make an uberjar (lein uberjar)
and run it with "java -jar UBERJAR.jar"

## License

Copyright © 2015 Robinhood.

Distributed under the Apache License, Version 2.0.
http://www.apache.org/licenses/LICENSE-2.0
