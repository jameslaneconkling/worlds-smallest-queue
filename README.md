# World's Smallest Queue
A stupid-simple Postgres-backed queue

## Goals
- **partitionable messages**: customizable partitioning of messages. transparently balance message throughput across each partition. efficiently expose all messages or message counts per partition.
- **scalable enqueueing**: reliably handle large message spikes
- **lightweight management**: minimize operational overhead
- **minimal surface area**: under 200 LOC

## Non-Goal
- **maximize message throughput or latency**: this library is intended for situations where the queue is fronting expensive business logic like a complex query to a resource-constrained system. the queue should handle significant message spikes and effectively rate limit message processing, but is not intended for scenarios where queue throughput itself is expected to be the main bottleneck.

## Terminology
- message: a single serializable datastructure to enqueue and dequeue
- partition: an arbitrary subset of messages defined during message creation. as long as partitions remain reletively small/high cardinality, retrieving counts and lists of unprocessed messages per partition should remain efficient
- messenger: a function that dequeues messages, processes them, re-enqueues them on error, and deletes them on success. scale concurrent messengers up or down to easily manage throughput
