# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Parcl Labs Python SDK — official Python client for the Parcl Labs real estate data API. Wraps REST endpoints into service classes that return pandas DataFrames. Supports 70,000+ US housing markets.

## Commands

```bash
make lint          # ruff check --fix + ruff format
make lint-check    # ruff check + ruff format --check (used in CI)
make test          # python3 -m pytest -v
make test-readme   # extract and run code examples from README
```

Run a single test:
```bash
python3 -m pytest tests/test_parcl_labs_service.py -v
python3 -m pytest tests/test_parcl_labs_service.py::test_function_name -v
```

## Architecture

### Client → ServiceGroup → Service

`ParclLabsClient` is the entry point. It organizes endpoints into `ServiceGroup` instances, each containing multiple service objects registered via `add_service()`. Users access services as chained attributes: `client.market_metrics.housing_event_prices.retrieve(...)`.

### Service Hierarchy

All services inherit from `ParclLabsService` (in `parcllabs/services/parcllabs_service.py`), which handles HTTP requests, pagination, error handling, and DataFrame conversion.

Two intermediate subclasses add a single parameter each:
- `PropertyTypeService` — adds `property_type` param (SINGLE_FAMILY, CONDO, TOWNHOUSE, etc.)
- `PortfolioSizeService` — adds `portfolio_size` param (PORTFOLIO_2_TO_9, PORTFOLIO_10_TO_99, etc.)

Specialized services for property-level operations:
- `PropertySearch`, `PropertyAddressSearch` — lookup by parcl_id or address
- `PropertyEventsService` — event history (sales, listings, rentals)
- `PropertyV2Service` — advanced search with chunking, concurrent pagination via ThreadPoolExecutor, and Pydantic validation
- `SearchMarkets` — market discovery with location_type, region, state filters

### Request Flow

1. `service.retrieve(parcl_ids, start_date, end_date, ...)` validates inputs and chunks parcl_ids into batches of 1000
2. `_fetch()` routes to POST (if `post_url` configured) or GET
3. `_process_and_paginate_response()` follows `links.next` for auto-pagination
4. `_as_pd_dataframe()` normalizes JSON via `pd.json_normalize()`, reorders columns (parcl_id/date first), casts date types

### Key Limits

- GET: max 1000 per request (`RequestLimits.DEFAULT_SMALL`)
- POST: max 10000 per request (`RequestLimits.DEFAULT_LARGE`)
- PropertyV2: max 50000 (`RequestLimits.PROPERTY_V2_MAX`)
- parcl_ids chunked in batches of 1000 in `retrieve()`

## Code Conventions

- **Linting**: ruff with line-length=100, target py311. See `ruff.toml` for selected rules.
- **Type hints**: required on all functions (ruff ANN rules enforced). Use `str | None` union syntax.
- **Enums**: defined in `parcllabs/enums.py`. User input is uppercased before matching.
- **Validation**: `parcllabs/services/validators.py` — date format (YYYY-MM-DD), zip codes (5 digits), boolean params converted to string "true"/"false".
- **Error types**: `NotFoundError` (404), `DataValidationError` (422), both inherit `ParclLabsError`. Defined in `parcllabs/exceptions.py`.
- **Version**: single source of truth in `parcllabs/__version__.py`.

## Testing Patterns

- Tests use `unittest.mock` to patch service internals (`_fetch_get`, `_fetch_post`)
- Shared fixtures in `tests/conftest.py`: `client_mock`, `service`, `api_key`, `client`
- One test file per service (e.g., `tests/test_market_metrics_service.py`)
- CI runs on Python 3.11, ubuntu-latest. Requires `PARCL_LABS_API_KEY` secret for integration tests.

## Adding a New Service

1. Create service class inheriting from `ParclLabsService` (or `PropertyTypeService`/`PortfolioSizeService` if it needs those params)
2. Register it in the appropriate `_create_*_services()` method in `parcllabs/parcllabs_client.py` with url, post_url, and service_class
3. Add tests in `tests/test_*.py`
