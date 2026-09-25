# felix-kafka

The Kafka wire protocol over a Felix broker: `ApiVersions`, SASL/PLAIN,
`Metadata`, `ListOffsets`, a long-polling `Fetch`, and `Produce` with
`InitProducerId` for idempotent producers. Consumer groups and transactions are
refused with an error a client can print.

The broker service owns the socket, TLS and configuration and hands each
accepted connection to `KafkaService`. What works, what does not, and why is in
[docs/kafka-compatibility.md](../../../docs/kafka-compatibility.md).
