# history APIs

Source: `src/checkpoint_platform/interfaces/http/routes/history.py`

## GET /history/aggregations/counter/<counter_id>

- Handler: `counter_agg_history`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /history/aggregations/cafe/<cafe_id>

- Handler: `cafe_agg_history`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /history/aggregations/client/<client_id>

- Handler: `client_agg_history`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /history/events/checkpoint/<checkpoint_id>

- Handler: `checkpoint_versions`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /history/events/counter/<counter_id>

- Handler: `counter_events`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /history/events/cafe/<cafe_id>

- Handler: `cafe_events`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /checkpoints/state/<checkpoint_id>

- Handler: `checkpoint_state`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /checkpoints/state/counter/<counter_id>

- Handler: `counter_states`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /audit/processed/<event_id>

- Handler: `audit_processed`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /audit/processed/checkpoint/<checkpoint_id>

- Handler: `audit_processed_checkpoint`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /reporting/counter/<counter_id>

- Handler: `reporting_counter`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /ops/outbox

- Handler: `outbox_stats`
- File: `src/checkpoint_platform/interfaces/http/routes/history.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)
