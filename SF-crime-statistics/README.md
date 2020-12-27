# SF Crime Statistics

This project defines a pipeline between Apache Kafka and Apache Spark Structured Streaming. Kafka reads information about crime-related calls to the San Francisco polics from a large JSON file and then publishes it as a topic. Spark Streaming subscribes to this topic and performs some aggregations and joins on the data.

## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Without any parameter tuning (and *maxOffsetsPerTrigger* set to 200) the throughput os as follows:

```json
  "numInputRows" : 199,
  "processedRowsPerSecond" : 7.5310323947926125,
  "durationMs" : {
    "addBatch" : 22016,
    "getBatch" : 270,
    "getOffset" : 3363,
    "queryPlanning" : 701,
    "triggerExecution" : 26423,
    "walCommit" : 50
  }
````

Improving data locality by setting *spark.sql.shuffle.partitions* to 1 results in a noticeable improvement

```json
  "numInputRows" : 199,
  "processedRowsPerSecond" : 21.576493548736853,
  "durationMs" : {
    "addBatch" : 4890,
    "getBatch" : 296,
    "getOffset" : 3312,
    "queryPlanning" : 639,
    "triggerExecution" : 9222,
    "walCommit" : 54
  }
  ````

and setting *spark.default.parallelism* to 1 as well gives another boost

```json
  "numInputRows" : 199,
  "inputRowsPerSecond" : 21.071579839051246,
  "processedRowsPerSecond" : 158.94568690095846,
  "durationMs" : {
    "addBatch" : 951,
    "getBatch" : 9,
    "getOffset" : 13,
    "queryPlanning" : 215,
    "triggerExecution" : 1252,
    "walCommit" : 57
  }
  ```

Another improvement can be achieved by using *minRatePerPartition*

```json
  "numInputRows" : 199,
  "inputRowsPerSecond" : 136.39479095270733,
  "processedRowsPerSecond" : 207.07596253902187,
  "durationMs" : {
    "addBatch" : 793,
    "getBatch" : 6,
    "getOffset" : 4,
    "queryPlanning" : 125,
    "triggerExecution" : 961,
    "walCommit" : 29
  }
```

No gain is reached by increasing the available heap space using *spark.driver.memory*

```json
  "numInputRows" : 199,
  "inputRowsPerSecond" : 20.58123901127314,
  "processedRowsPerSecond" : 153.31278890600925,
  "durationMs" : {
    "addBatch" : 1003,
    "getBatch" : 7,
    "getOffset" : 9,
    "queryPlanning" : 190,
    "triggerExecution" : 1298,
    "walCommit" : 76
  }
```

## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

The most significant improvements could be reached by

- setting *spark.sql.shuffle.partitions* and *spark.default.parallelism* to 1 (although this is probably particular to the workspace environment that only uses one executor)

- *minRatePerPartition* where I got the best throughput with values around 400
