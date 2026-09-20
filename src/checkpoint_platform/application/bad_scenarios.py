"""
Production bad-scenario catalog (15+).

Each scenario is a named failure mode interviewers / ops care about.
Executable checks live in tests/test_prod_bad_scenarios.py and scripts/chaos_suite.py.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BadScenario:
    id: str
    title: str
    what_goes_wrong: str
    expected_behavior: str


# At least 15 production failure modes — keep IDs stable for reporting.
PROD_BAD_SCENARIOS: tuple[BadScenario, ...] = (
    BadScenario(
        id="S01_duplicate_event_id",
        title="Duplicate Kafka delivery (same event_id)",
        what_goes_wrong="Consumer crashes after DB commit; Kafka redelivers same event_id",
        expected_behavior="Second apply raises DuplicateEventError / skip; aggregate unchanged",
    ),
    BadScenario(
        id="S02_wastage_delta_correction",
        title="checkpoint.updated quantity correction",
        what_goes_wrong="Wastage reported 20kg then corrected to 15kg",
        expected_behavior="Aggregate becomes 15 via delta (not 20+15)",
    ),
    BadScenario(
        id="S03_stale_version",
        title="Stale checkpoint_version after newer state",
        what_goes_wrong="Old version arrives after a higher version was applied",
        expected_behavior="StaleVersionError; current state retained",
    ),
    BadScenario(
        id="S04_out_of_order_versions",
        title="Out-of-order versions (v3 then v2)",
        what_goes_wrong="Offline sync / network reordering",
        expected_behavior="v2 ignored; value stays at v3",
    ),
    BadScenario(
        id="S05_crash_before_commit",
        title="DB work rolled back before commit (crash analogue)",
        what_goes_wrong="Process succeeds in memory then session.rollback (crash before commit)",
        expected_behavior="Replay with same event_id applies exactly once",
    ),
    BadScenario(
        id="S06_malformed_json",
        title="Malformed JSON payload",
        what_goes_wrong="Corrupt bytes / truncated message",
        expected_behavior="EventProcessor → DLQ (not crash consumer loop)",
    ),
    BadScenario(
        id="S07_schema_validation",
        title="Schema validation failure",
        what_goes_wrong="JSON parses but missing required fields (counter_id, etc.)",
        expected_behavior="ValidationError → DLQ",
    ),
    BadScenario(
        id="S08_transient_db_retry",
        title="Transient DB / network error",
        what_goes_wrong="Timeout / connection reset mid-processing",
        expected_behavior="Classified transient → retry topic with incremented retry_count",
    ),
    BadScenario(
        id="S09_max_retries_dlq",
        title="Max retries exhausted",
        what_goes_wrong="Same transient failure repeats beyond MAX_RETRIES",
        expected_behavior="Publish to DLQ; stop retrying",
    ),
    BadScenario(
        id="S10_permanent_no_retry",
        title="Permanent validation error",
        what_goes_wrong="Business/validation rule permanently broken",
        expected_behavior="DLQ immediately; not retried as transient",
    ),
    BadScenario(
        id="S11_empty_counter_id",
        title="Empty / invalid partition key fields",
        what_goes_wrong="counter_id empty string",
        expected_behavior="Pydantic ValidationError; rejected before aggregation",
    ),
    BadScenario(
        id="S12_status_flip_completed_to_failed",
        title="Status flip COMPLETED → FAILED",
        what_goes_wrong="Checkpoint marked done then failed",
        expected_behavior="completed_checkpoints decreases; failed_checkpoints increases",
    ),
    BadScenario(
        id="S13_hygiene_pass_then_fail",
        title="Hygiene pass then fail",
        what_goes_wrong="STAFF_HYGIENE PASS updated to FAIL",
        expected_behavior="hygiene_pass_count -1; hygiene_fail_count +1",
    ),
    BadScenario(
        id="S14_reingest_new_event_id",
        title="DLQ re-ingest with new event_id",
        what_goes_wrong="Poison fixed; operator re-publishes from DLQ",
        expected_behavior="New event_id minted so idempotency allows reprocessing",
    ),
    BadScenario(
        id="S15_retry_backoff_metadata",
        title="Retry exponential backoff metadata",
        what_goes_wrong="Retry storm without delay",
        expected_behavior="retry_count++ and next_retry_at set; delay grows then caps",
    ),
    BadScenario(
        id="S16_prepared_consumed_wastage_consistency",
        title="Multiple quantity types on same counter/meal",
        what_goes_wrong="Prepared / consumed / wastage updates interleave",
        expected_behavior="Each quantity column tracks its own deltas independently",
    ),
    BadScenario(
        id="S17_same_version_replay_different_event_id",
        title="Same version, different event_id (bad producer)",
        what_goes_wrong="Producer retries with new event_id but same checkpoint_version",
        expected_behavior="Treated as stale (version <= current); aggregate not double-counted",
    ),
    BadScenario(
        id="S18_is_transient_classification_matrix",
        title="Error classification matrix",
        what_goes_wrong="Wrong retry vs DLQ decision",
        expected_behavior="timeouts/ops errors → transient; validation → permanent",
    ),
    BadScenario(
        id="S19_unit_mismatch_grams",
        title="Device reports grams instead of kilograms",
        what_goes_wrong="Scale configured in G sends 45000; naive code adds 45000kg",
        expected_behavior="Normalized to 45kg before aggregation; unknown units → DLQ",
    ),
    BadScenario(
        id="S20_meal_type_correction",
        title="Correction moves checkpoint to another meal",
        what_goes_wrong="Operator filed a DINNER checkpoint under LUNCH and corrects it",
        expected_behavior="LUNCH row reversed to zero; DINNER row receives the value",
    ),
    BadScenario(
        id="S21_checkpoint_type_correction",
        title="Correction changes checkpoint_type",
        what_goes_wrong="FOOD_PREPARED 100kg corrected to FOOD_WASTAGE 5kg",
        expected_behavior="food_prepared_kg returns to 0; food_wastage_kg becomes 5",
    ),
    BadScenario(
        id="S22_cross_day_correction",
        title="Near-midnight correction lands on another business day",
        what_goes_wrong="v1 recorded late on day 1, v2 arrives on day 2",
        expected_behavior="Day 1 row reversed; day 2 row holds the corrected value",
    ),
    BadScenario(
        id="S23_business_day_boundary",
        title="Business-day boundary vs UTC",
        what_goes_wrong="23:40 IST event buckets into the previous UTC day",
        expected_behavior="aggregation_date resolved in the configured business timezone",
    ),
    BadScenario(
        id="S24_future_clock_skew",
        title="Device clock skew far into the future",
        what_goes_wrong="Faulty RTC stamps occurred_at in 2099, creating a phantom day",
        expected_behavior="Rejected as permanent validation error → DLQ",
    ),
    BadScenario(
        id="S25_absurd_and_negative_values",
        title="Negative or impossible quantities",
        what_goes_wrong="Broken sensor reports -5kg or 10^9 kg of wastage",
        expected_behavior="Rejected by domain validation → DLQ, aggregates untouched",
    ),
    BadScenario(
        id="S26_empty_or_tombstone_payload",
        title="Null / zero-length Kafka record",
        what_goes_wrong="Compacted tombstone or truncated produce",
        expected_behavior="DLQ with empty_payload reason; consumer loop survives",
    ),
    BadScenario(
        id="S27_dlq_when_broker_unreachable",
        title="Poison message while the broker is unreachable",
        what_goes_wrong="DLQ publish fails, so the offset never advances",
        expected_behavior="DLQ row persisted to Postgres first; offset still commits",
    ),
    BadScenario(
        id="S28_batch_isolation",
        title="One poison event inside a large batch",
        what_goes_wrong="A batch transaction is discarded, losing good events with it",
        expected_behavior="Per-message savepoints keep good events; batch still commits",
    ),
    BadScenario(
        id="S29_stale_identity_map_in_batch",
        title="Two versions of one checkpoint in the same batch",
        what_goes_wrong="ORM returns the pre-upsert row, so the delta is applied twice",
        expected_behavior="State re-read FOR UPDATE with populate_existing; totals exact",
    ),
    BadScenario(
        id="S30_concurrent_duplicate_delivery",
        title="Same event delivered to two consumers during a rebalance",
        what_goes_wrong="Both pass an exists() check and both apply the event",
        expected_behavior="Atomic INSERT ... ON CONFLICT claim lets exactly one apply",
    ),
    BadScenario(
        id="S31_retry_not_due_blocks_partition",
        title="Retry backoff stalls the partition",
        what_goes_wrong="Consumer sleeps until a retry is due, halting throughput",
        expected_behavior="Not-due retries are requeued; retry_count is not incremented",
    ),
    BadScenario(
        id="S32_retention_growth",
        title="Idempotency and audit tables grow without bound",
        what_goes_wrong="72M rows/day at 50k/min degrades the hottest lookup",
        expected_behavior="Maintenance worker prunes by retention; pending DLQ retained",
    ),
    BadScenario(
        id="S33_hostile_retry_count",
        title="Non-numeric retry_count from a hostile producer",
        what_goes_wrong='retry_count: "many" raises inside the consumer loop',
        expected_behavior="Coerced to 0; message still routed normally",
    ),
)


def scenario_ids() -> list[str]:
    return [s.id for s in PROD_BAD_SCENARIOS]


def require_min_count(n: int = 15) -> None:
    if len(PROD_BAD_SCENARIOS) < n:
        raise RuntimeError(f"Need >= {n} bad scenarios, have {len(PROD_BAD_SCENARIOS)}")
