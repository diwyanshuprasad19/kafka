# dlq APIs

Source: `src/checkpoint_platform/interfaces/http/routes/dlq.py`

## GET /dlq

- Handler: `list_dlq`
- File: `src/checkpoint_platform/interfaces/http/routes/dlq.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /dlq/<int:dlq_id>

- Handler: `get_dlq`
- File: `src/checkpoint_platform/interfaces/http/routes/dlq.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)
