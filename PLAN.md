# Design and roadmap

## Design

| Topic | Decision | Why |
|---|---|---|
| Keys | The MMSI on every position topic | Kafka orders records only within a partition; one key → one partition → each vessel's reports in order |
| Partitions | 6 per main topic | Up to 6 consumers per group share the work; enough to see rebalancing with two instances |
| Replication | 3 replicas, `min.insync.replicas=2`, producers `acks=all` + idempotence | Survives one broker down with no data loss; refuses writes rather than risk loss with two down |
| Cluster | 3 nodes in KRaft combined mode | Three controller voters tolerate one failure; no ZooKeeper (removed in Kafka 4.0) |
| Topic creation | By the app (`KafkaTopicsConfig`); broker auto-create off | Every topic's settings in one place, never created by a typo |
| Format | Plain JSON, no type headers, ISO timestamps | Readable in the UI and by consumers in any language |
| Cleaner delivery | At least once; forward synchronously, then commit | No report is lost; cleaning is idempotent, so a repeat is harmless |
| Bad records | Dead-letter topic; validation errors and non-JSON records are not retried | One bad record must not block a partition |
| Streams delivery | `exactly_once_v2` | State store updates and outputs commit together |
| Event time | `AisTimestampExtractor` reads the AIS time; record timestamps stay at send time | Windows follow the ships' clock; retention still works for replayed 2020 data |
| Port areas | Circles with a 0.5 nm leave margin | GPS wobble near the edge does not produce enter/leave noise |
| State | Kafka Streams stores (RocksDB) with changelog topics; no database | Queried over REST (interactive queries) |

## Roadmap

**Connecting other services**

- The berth planner (`ship-eta-berth-scheduler`) could consume `port.zone-events`
  to know which ships are approaching.
- The ETA model could consume `ais.positions` and publish predictions to a new
  `eta.predictions` topic.
- The vessel operations orchestrator could start or signal a visit when a
  ship enters its port area.

**Pipeline**

- Schema Registry with Avro or Protobuf, and compatibility rules for changing a schema.
- Interactive queries across instances: route a query to the instance that
  holds the vessel's partition.
- Final hourly counts only (`suppress(untilWindowCloses)`) instead of updates.
- Exactly-once in the cleaner too (a transactional consume-transform-produce).
- Kafka Connect: a sink that writes `vessel.latest` to a database.
- Metrics: consumer lag and throughput in Prometheus and Grafana.
- Security: TLS and SASL on the listeners, and ACLs per service.
- Arrival detection from navigational status as well as position (moored and stopped).
