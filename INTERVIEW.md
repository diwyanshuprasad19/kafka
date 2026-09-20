# Interview cheat sheet — Checkpoint Aggregation Platform

## Why Kafka?
Decoupling, async aggregation, spike buffering (meal peaks + offline sync bursts), independent consumer scaling, multiple subscribers (aggregation + reporting).

## Producer
Flask `POST /checkpoints` and `scripts/generate_events.py` → `CheckpointProducer` (`acks=all`, `enable.idempotence=true`).

## Consumer
`checkpoint-aggregation-service` group; reads `checkpoint.events.v1` + `checkpoint.retry.v1`; manual offset commit after DB success.

## Partition key
`counter_id` — ordering per counter + good distribution across 30K counters.

## Partitions
6 locally (replication factor 1). Production RF typically 3.

## Peak events/sec
Measure on your laptop with `generate_events.py --rate N`. Do not invent numbers in interviews.

## Duplicates
`processed_events.event_id` PK. Second delivery → skip, still commit offset.

## Offset commit timing
After successful DB transaction (or duplicate/stale/dlq/retry handling). Never before DB commit.

## DB success + offset commit fail
Message redelivered; idempotency makes replay safe.

## Retry
Transient → `checkpoint.retry.v1` with `retry_count`. After `MAX_RETRIES` → DLQ.

## DLQ
`checkpoint.dlq.v1` + `dlq_records` table with original payload, error, partition, offset.

## checkpoint.updated
Load previous `checkpoint_state`, compute deltas, UPSERT aggregation atomically.

## Prevent wrong aggregation
Version checks + deltas + unique event_id + single DB transaction + atomic SQL UPSERT.

## Consumer lag
Prometheus / Kafka tooling: if produce rate > consume rate, lag grows. Scale consumers up to partition count; optimize DB; increase partitions if needed.

## Outbox
Aggregation row + `outbox_events` in same transaction → publisher drains to `aggregation.completed.v1`.
