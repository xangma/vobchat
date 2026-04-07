# Runtime architecture

This is the maintainer-facing summary of the active VobChat runtime after the rewrite.

## Boundaries

The active codebase is split into five runtime areas:

- [`src/vobchat/web/`](../src/vobchat/web): Dash application shell, user-facing auth integration, browser stores, and same-origin chat proxy routes
- [`src/vobchat/api/`](../src/vobchat/api): FastAPI application, typed routers, orchestrator, and SSE endpoints
- [`src/vobchat/core/`](../src/vobchat/core): settings, logging, provider-neutral OpenAI-compatible LLM client, planner prompts and schemas
- [`src/vobchat/db/`](../src/vobchat/db): SQLAlchemy engine/session layer and typed repositories
- [`src/vobchat/auth/`](../src/vobchat/auth): Flask-Login auth models, routes, templates, and CLI commands

The old LangGraph/Redis workflow architecture has been removed.

## Request flow

### Chat

1. The browser loads the Dash app from the web service.
2. Browser chat creates a thread and submits turns through same-origin proxy routes under `/proxy/chat/*`.
3. The web service authenticates the browser session and relays those requests to the internal FastAPI service.
4. The FastAPI chat orchestrator plans a typed operation, calls deterministic services/repositories, emits SSE events, and returns structured UI deltas plus assistant text.
5. The browser applies assistant text and structured `ui.delta` payloads without parsing assistant prose.

### Non-chat data flows

Maps, places, themes, series, and metadata still go through the typed API boundary, but the current runtime uses server-side Dash callbacks plus the typed web API client for those requests.

## Why the browser talks to the web layer for chat

Chat is intentionally same-origin through the web layer because:

- the existing browser auth model is Flask session cookies
- the FastAPI service is treated as an internal backend
- the web proxy can enforce authenticated thread ownership before relaying chat traffic

This keeps browser auth simple and avoids a second public API auth scheme.

## LLM runtime boundary

The app talks to an external or local OpenAI-compatible endpoint through the provider-neutral settings in [`src/vobchat/core/settings.py`](../src/vobchat/core/settings.py).

Important constraints:

- model weights are not bundled into the main app image
- the default `docker-compose.yml` stack does not run a model server
- the browser never talks directly to the model server
- the recommended user-facing setup path is `vobchat setup-llm`

Supported modes today:

- existing OpenAI-compatible endpoint
- local Ollama setup
- optional separate vLLM helper stack via [`docker-compose.llm-vllm.yml`](../docker-compose.llm-vllm.yml)

## State

### Browser-visible state

The Dash runtime keeps a small number of `dcc.Store` objects in session storage for:

- thread state
- selection state
- map state
- visualization state
- metadata state
- request status

These stores exist to render the UI and survive page reloads within the browser session.

### Backend chat state

Chat thread state lives in the in-memory store implemented in [`src/vobchat/api/services/chat_threads.py`](../src/vobchat/api/services/chat_threads.py).

That store owns:

- thread metadata
- transcript/messages
- selected places
- selected theme
- latest UI delta
- SSE subscriber queues

It is intentionally in-memory for now.

### Proxy ownership state

The authenticated same-origin proxy keeps an in-memory thread ownership registry in [`src/vobchat/web/proxy/routes.py`](../src/vobchat/web/proxy/routes.py). That registry binds proxied chat threads to the current authenticated web session.

## Data access

All rewrite-path database access goes through:

- [`src/vobchat/db/engine.py`](../src/vobchat/db/engine.py)
- [`src/vobchat/db/session.py`](../src/vobchat/db/session.py)
- repositories under [`src/vobchat/db/repositories/`](../src/vobchat/db/repositories)

Important constraints:

- SQL is parameterized
- queries are read-only
- the model does not generate SQL
- the web runtime does not query the database directly

## Entrypoints

Primary entrypoints:

- Web: `vobchat.web.app:server`
- API: `vobchat.api.main:app`

Compatibility entrypoint:

- `vobchat.app:server`

That compatibility module is intentionally tiny and only delegates to the real web app.

## What remains intentionally unsolved

- Provenance/transparency is still incomplete. The runtime returns useful typed datasets and metadata, but not a full provenance bundle for plotted data.
- Chat thread persistence is still in-memory and not durable across API restarts.
- Non-chat browser traffic still uses server-side Dash callbacks rather than broader same-origin proxying, which is acceptable for the current runtime shape.
