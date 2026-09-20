from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
)

# --- Core processing ---
EVENTS_PROCESSED = Counter(
    "checkpoint_events_processed_total",
    "Checkpoint events processed by outcome",
    ["outcome"],
)

DUPLICATE_EVENTS = Counter(
    "checkpoint_duplicate_events_total",
    "Duplicate Kafka events skipped via processed_events",
)

STALE_EVENTS = Counter(
    "checkpoint_stale_events_total",
    "Out-of-order / stale version events ignored",
)

RETRY_COUNT = Counter(
    "checkpoint_retry_total",
    "Events published to retry topic",
)

DLQ_COUNT = Counter(
    "checkpoint_dlq_total",
    "Events published to DLQ",
)

DLQ_REASONS = Counter(
    "checkpoint_dlq_reasons_total",
    "DLQ routing decisions by reason",
    ["reason"],
)

PROCESSING_LATENCY = Histogram(
    "checkpoint_processing_seconds",
    "End-to-end processing latency per event",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

CONSUMER_LAG = Gauge(
    "checkpoint_consumer_lag",
    "Approximate consumer lag (messages)",
    ["topic", "partition"],
)

OUTBOX_PUBLISHED = Counter(
    "outbox_events_published_total",
    "Outbox events successfully published to Kafka",
)

OUTBOX_PENDING = Gauge(
    "outbox_events_pending",
    "Unpublished outbox rows (aggregation fan-out backlog)",
)

DLQ_PENDING = Gauge(
    "checkpoint_dlq_pending",
    "DLQ records awaiting operator triage / re-ingestion",
)

CONSUMER_BATCH_SIZE = Histogram(
    "checkpoint_consumer_batch_size",
    "Messages handled per DB transaction / offset commit",
    buckets=(1, 5, 10, 25, 50, 100, 200, 500, 1000),
)

BATCH_FALLBACKS = Counter(
    "checkpoint_consumer_batch_fallback_total",
    "Batches replayed message-by-message after a batch-level failure",
)

ROWS_PRUNED = Counter(
    "checkpoint_rows_pruned_total",
    "Rows removed by the retention maintenance worker",
    ["table"],
)

ROLLUP_ROWS = Counter(
    "checkpoint_rollup_rows_total",
    "Cafe/client rollup rows restated from the counter grain",
    ["level"],
)

# Cafe and client figures trail the counter grain by design; this is by how much.
ROLLUP_LAG_SECONDS = Gauge(
    "checkpoint_rollup_lag_seconds",
    "Age of the rollup watermark — how stale cafe/client aggregates are",
)

PRODUCER_EVENTS = Counter(
    "checkpoint_producer_events_total",
    "Events produced by the load generator / API",
    ["source"],
)

REINGEST_EVENTS = Counter(
    "checkpoint_reingest_total",
    "Re-ingestion attempts by source and outcome",
    ["source", "outcome"],
)

REINGEST_FAILURES = Counter(
    "checkpoint_reingest_failures_total",
    "Re-ingestion failures",
)

HTTP_REQUESTS = Counter(
    "http_requests_total",
    "HTTP requests",
    ["method", "endpoint", "status"],
)

HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# --- Operational gauges for 50k/min prod scale ---
EVENTS_PER_MINUTE = Gauge(
    "checkpoint_events_per_minute",
    "Rolling processed events per minute (approx)",
    ["service"],
)

TARGET_EVENTS_PER_MINUTE = Gauge(
    "checkpoint_target_events_per_minute",
    "Configured production throughput target (events/minute)",
)

DB_ERRORS = Counter(
    "checkpoint_db_errors_total",
    "Database errors during processing",
)

KAFKA_PUBLISH_ERRORS = Counter(
    "checkpoint_kafka_publish_errors_total",
    "Kafka publish failures",
)

ACTIVE_CONSUMERS = Gauge(
    "checkpoint_active_consumer_info",
    "Consumer process heartbeat (1=up)",
    ["instance"],
)

APP_INFO = Info(
    "checkpoint_platform",
    "Application build/runtime info",
)


def start_metrics_server(port: int) -> None:
    from prometheus_client import start_http_server

    start_http_server(port)


def metrics_payload() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST


def set_app_info(*, env: str, service: str, version: str = "0.1.0") -> None:
    from checkpoint_platform.infrastructure.messaging.tuning import TARGET_EVENTS_PER_MINUTE as TARGET

    APP_INFO.info({"env": env, "service": service, "version": version})
    TARGET_EVENTS_PER_MINUTE.set(TARGET)
