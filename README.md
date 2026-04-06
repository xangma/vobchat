# VobChat

VobChat is an authenticated conversational interface for exploring Vision of Britain and GBHGIS-style historical data with chat, maps, charts, and tabular outputs.

The active runtime is the rewritten stack completed on the `codex/fastapi-semantic-layer-plan` branch:

- Dash web runtime for the user-facing application shell
- FastAPI backend for typed domain APIs and chat orchestration
- SQLAlchemy + psycopg3 repository layer over PostgreSQL/PostGIS
- OpenAI-compatible local-model client, with Ollama as the default local endpoint

The old LangGraph/Redis prototype runtime has been removed. The only retained legacy-facing entrypoint is [`src/vobchat/app.py`](src/vobchat/app.py), which is now a tiny wrapper around the real web app in [`src/vobchat/web/app.py`](src/vobchat/web/app.py).

## Status

The rewrite runtime is active and feature-complete for the main chat, place, theme, map, chart, table, metadata, auth, and same-origin streaming flows.

Known limitation:

- Provenance/transparency is still incomplete. The app returns typed datasets and metadata, but it does not yet expose a full provenance bundle for plotted data.

See [`docs/rearchitecture/parity-and-progress.md`](docs/rearchitecture/parity-and-progress.md) for the detailed parity and cleanup tracker, and [`docs/runtime-architecture.md`](docs/runtime-architecture.md) for the maintainer-facing architecture summary.

## Architecture

The repo is organized around five active runtime areas:

- [`src/vobchat/web/`](src/vobchat/web): Dash app, layout, callbacks, typed API client, and authenticated same-origin proxy routes
- [`src/vobchat/api/`](src/vobchat/api): FastAPI app, routers, typed schemas, services, chat SSE endpoints
- [`src/vobchat/core/`](src/vobchat/core): typed settings, shared logging, local-model client, planner, prompts
- [`src/vobchat/db/`](src/vobchat/db): SQLAlchemy engine/session setup, typed repository layer, semantic SQL assets
- [`src/vobchat/auth/`](src/vobchat/auth): Flask-Login auth models, routes, templates, and CLI commands

High-level runtime flow:

1. The browser talks to the Dash web service.
2. Browser chat uses same-origin proxy routes under `/proxy/chat/*`.
3. The web service relays chat requests and SSE streams to the internal FastAPI API.
4. The API runs the planner/orchestrator, calls typed services and repositories, and streams assistant/UI events back.
5. Maps, series, themes, places, and metadata all resolve through the typed API/repository stack rather than direct DB access from Dash callbacks.

## Local Development

### Prerequisites

- Python 3.9+
- PostgreSQL/PostGIS access to the Vision of Britain data
- An OpenAI-compatible local model endpoint
  - Ollama is the default target and expected to expose an OpenAI-compatible `/v1` surface

### Environment setup

Copy the example environment file and adjust the values:

```bash
cp .env.example .env
```

Important variables:

- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_SCHEMA`
- `OLLAMA_HOST`, `OLLAMA_PORT`, `OLLAMA_SUBPATH`, `OLLAMA_USE_SSL`
- `VOBCHAT_LLM_MODEL`
- `SECRET_KEY`
- `AUTH_DATABASE_URL`
- `VOBCHAT_API_BASE_URL`

For local two-process development, set:

```bash
VOBCHAT_API_BASE_URL=http://127.0.0.1:8000
SESSION_COOKIE_SECURE=false
OLLAMA_USE_SSL=false
```

### Install dependencies

Preferred development install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Alternative runtime-only install:

```bash
pip install -r requirements.txt
```

Packaging note:

- [`pyproject.toml`](pyproject.toml) is the canonical package metadata.
- [`requirements.txt`](requirements.txt) mirrors the runtime dependency set for plain `pip` installs and container builds.

### Run the API

```bash
uvicorn vobchat.api.main:app --reload --host 127.0.0.1 --port 8000
```

### Run the web app

Preferred real entrypoint:

```bash
python -m vobchat.web.app
```

Compatibility entrypoint:

```bash
python -m vobchat.app
```

The compatibility module exists only to preserve `vobchat.app:server` style entrypoints. The real implementation lives in `vobchat.web.app`.

### Auth database setup

Initialise the auth database:

```bash
flask --app vobchat.web.app:server init-db
```

Create or reset a user:

```bash
flask --app vobchat.web.app:server add-user you@example.com
```

### Development chat/proxy behavior

In development, the browser still talks only to the web service for chat:

- browser -> Dash web app
- Dash proxy route -> FastAPI `/chat/*`
- FastAPI SSE -> web proxy relay -> browser `EventSource`

That means the browser does not need direct API credentials or a separate FastAPI login flow.

## Docker and Compose

The Compose topology reflects the active architecture:

- `web`: public Dash service on `127.0.0.1:8050`
- `api`: internal FastAPI service on port `8000` inside the Compose network

External services still expected:

- PostgreSQL/PostGIS
- OpenAI-compatible local model endpoint, with Ollama as the default target

### Start the stack

```bash
docker compose up --build
```

Detached mode:

```bash
docker compose up -d --build
```

### Service interaction

- The browser connects to `web`.
- Browser chat uses same-origin `/proxy/chat/*` routes on `web`.
- `web` forwards chat requests to `api` using `VOBCHAT_API_BASE_URL=http://api:8000`.
- `api` is not published as a primary browser-facing service in Compose.

### Auth setup in Compose

Initialise the auth database from the `web` container:

```bash
docker compose exec web flask --app vobchat.web.app:server init-db
```

Create or reset a user:

```bash
docker compose exec web flask --app vobchat.web.app:server add-user you@example.com
```

### Useful Compose commands

```bash
docker compose logs -f web
docker compose logs -f api
docker compose ps
docker compose down
```

## Entry points

Runtime entrypoints:

- Web: `vobchat.web.app:server`
- API: `vobchat.api.main:app`

Compatibility wrapper:

- `vobchat.app:server` delegates immediately to `vobchat.web.app:server`

Auth CLI commands:

- `flask --app vobchat.web.app:server init-db`
- `flask --app vobchat.web.app:server add-user EMAIL`

## Validation

Focused cleanup/import check:

```bash
PYTHONPATH=src pytest -q test_cleanup_phase8.py
```

Full targeted validation suite used on the rewrite branch:

```bash
python -m compileall src/vobchat test_cleanup_phase8.py
PYTHONPATH=src pytest -q \
  test_phase1_smoke.py \
  test_db_engine_phase2.py \
  test_db_repositories_phase2.py \
  test_api_phase3.py \
  test_chat_phase4.py \
  test_web_phase5.py \
  test_proxy_phase6.py \
  test_parity_phase7.py \
  test_cleanup_phase8.py
```

Optional Compose config validation:

```bash
docker compose config
```

## Current limitations

- Provenance/transparency is not fully implemented. The UI can display the returned datasets and metadata, but there is no full provenance bundle for plotted data yet.
- Chat thread state is intentionally in-memory in the API service. Reload continuity works within a running process, but chat state is not durable across backend restarts.
