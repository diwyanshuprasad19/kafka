# Kafka / checkpoint-platform API index

Generated from real Flask blueprints under `src/checkpoint_platform/interfaces/http/routes/`.
Do not invent endpoints — source of truth is code + `/metrics` OpenAPI is not used (Flask).

| Method | Endpoint | Handler | Source |
|--------|----------|---------|--------|
| GET | `/aggregations/cafe/<cafe_id>` | `get_cafe_aggregation` | `src/checkpoint_platform/interfaces/http/routes/aggregations.py` |
| GET | `/aggregations/client/<client_id>` | `get_client_aggregation` | `src/checkpoint_platform/interfaces/http/routes/aggregations.py` |
| GET | `/aggregations/counter/<counter_id>` | `get_counter_aggregation` | `src/checkpoint_platform/interfaces/http/routes/aggregations.py` |
| GET | `/audit/processed/<event_id>` | `audit_processed` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/audit/processed/checkpoint/<checkpoint_id>` | `audit_processed_checkpoint` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| POST | `/checkpoints` | `create_checkpoint` | `src/checkpoint_platform/interfaces/http/routes/checkpoints.py` |
| GET | `/checkpoints/state/<checkpoint_id>` | `checkpoint_state` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/checkpoints/state/counter/<counter_id>` | `counter_states` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/dlq` | `list_dlq` | `src/checkpoint_platform/interfaces/http/routes/dlq.py` |
| GET | `/dlq/<int:dlq_id>` | `get_dlq` | `src/checkpoint_platform/interfaces/http/routes/dlq.py` |
| GET | `/health` | `health` | `src/checkpoint_platform/interfaces/http/routes/health.py` |
| GET | `/history/aggregations/cafe/<cafe_id>` | `cafe_agg_history` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/history/aggregations/client/<client_id>` | `client_agg_history` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/history/aggregations/counter/<counter_id>` | `counter_agg_history` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/history/events/cafe/<cafe_id>` | `cafe_events` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/history/events/checkpoint/<checkpoint_id>` | `checkpoint_versions` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/history/events/counter/<counter_id>` | `counter_events` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/metrics` | `metrics` | `src/checkpoint_platform/interfaces/http/routes/metrics.py` |
| GET | `/ops/outbox` | `outbox_stats` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
| GET | `/ops/scale` | `ops_scale` | `src/checkpoint_platform/interfaces/http/routes/ops.py` |
| GET | `/ops/status` | `ops_status` | `src/checkpoint_platform/interfaces/http/routes/ops.py` |
| GET | `/ready` | `ready` | `src/checkpoint_platform/interfaces/http/routes/health.py` |
| POST | `/reingest/dlq` | `reingest_dlq_bulk` | `src/checkpoint_platform/interfaces/http/routes/reingest.py` |
| POST | `/reingest/dlq/<int:dlq_id>` | `reingest_dlq_one` | `src/checkpoint_platform/interfaces/http/routes/reingest.py` |
| POST | `/reingest/events` | `reingest_events` | `src/checkpoint_platform/interfaces/http/routes/reingest.py` |
| GET | `/reporting/counter/<counter_id>` | `reporting_counter` | `src/checkpoint_platform/interfaces/http/routes/history.py` |
