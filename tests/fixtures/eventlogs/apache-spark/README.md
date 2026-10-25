# Event logs from Apache Spark's own test suite

Copied unchanged from `core/src/test/resources/spark-events/` in [apache/spark](https://github.com/apache/spark),
licensed under the Apache License 2.0 (see `LICENSE-APACHE` in this folder). They cover log shapes the generated
fixtures can't: Spark 3.1 and 3.3, YARN with 10 executors, structured streaming, and a real speculative task.

| File | Spark | What it has |
|---|---|---|
| `app-20200706201101-0003.json` | 3.1.0-SNAPSHOT | standalone, 16 cores, SQL |
| `application_1628109047826_1317105.json` | 3.1.1.119 | YARN, 10 executors, a speculative task killed after 53 s |
| `local-1596020211915.json` | 3.1.0-SNAPSHOT | structured streaming (24 SQL executions) |
| `local-1642039451826.json` | 3.3.0-SNAPSHOT | AQE plan updates |
