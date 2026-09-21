# reingest APIs

Source: `src/checkpoint_platform/interfaces/http/routes/reingest.py`

## POST /reingest/dlq

- Handler: `reingest_dlq_bulk`
- File: `src/checkpoint_platform/interfaces/http/routes/reingest.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## POST /reingest/dlq/<int:dlq_id>

- Handler: `reingest_dlq_one`
- File: `src/checkpoint_platform/interfaces/http/routes/reingest.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## POST /reingest/events

- Handler: `reingest_events`
- File: `src/checkpoint_platform/interfaces/http/routes/reingest.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)
