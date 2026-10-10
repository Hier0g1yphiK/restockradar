# restockradar — Stock & Drop Monitor

Polls retailer product feeds (Shopify first) and emits Discord webhook alerts for
restocks, price drops, newly listed products, and pages going live.

- Python 3.12, fully type-annotated.
- `httpx` for HTTP, APScheduler for scheduling, SQLite for persistence.
- `ruff` + `mypy --strict` must pass (Req 19.2).

## Development

```bash
python -m venv .venv
.venv/Scripts/activate      # Windows
pip install -e ".[dev]"

ruff check .
mypy
pytest
```

The test suite is strictly offline: a `conftest.py` guard fails any test that
attempts real network access (Req 21.4).
