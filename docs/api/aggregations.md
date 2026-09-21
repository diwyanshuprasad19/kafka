# aggregations APIs

Source: `src/checkpoint_platform/interfaces/http/routes/aggregations.py`

## GET /aggregations/counter/<counter_id>

- Handler: `get_counter_aggregation`
- File: `src/checkpoint_platform/interfaces/http/routes/aggregations.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /aggregations/cafe/<cafe_id>

- Handler: `get_cafe_aggregation`
- File: `src/checkpoint_platform/interfaces/http/routes/aggregations.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)

## GET /aggregations/client/<client_id>

- Handler: `get_client_aggregation`
- File: `src/checkpoint_platform/interfaces/http/routes/aggregations.py`
- Auth: not explicitly enforced in route module (check middleware/app factory)
