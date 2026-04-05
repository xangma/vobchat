# Current state audit

This audit is based on the current `main` branch and is intended to anchor the rewrite in what the prototype actually does today.

## 1. Product capabilities that already exist

The current app already provides the main user-facing surfaces we need to preserve:

- authenticated login flow
- chat interface
- SSE-based streaming responses
- Dash Leaflet map with polygon loading
- charting and tabular data display
- local Ollama-backed LLM integration
- PostgreSQL-backed data access against Vision of Britain / GBHGIS-style tables

This is good news: we are **not** inventing a product from scratch. The rewrite should preserve these outcomes while replacing the implementation shape underneath them.

## 2. Current runtime architecture

### 2.1 Monolithic Dash + Flask app

The application entrypoint is a single Dash/Flask app that wires together:

- layout assembly
- auth setup
- workflow creation
- SSE routes
- API routes for polygons / map state
- callback registration

That makes `src/vobchat/app.py` the current composition root and also a concentration point for technical coupling.

### 2.2 LangGraph-centered orchestration

The current conversation and action flow is built around a LangGraph workflow with many nodes, a custom start router, interrupt handling, and Redis-backed checkpointing. The graph is compiled at startup and used as the control plane for user turns.

### 2.3 LLM planner mixed with existing node graph

The repo already has an LLM planner layer (`conversational_agent.py`) that emits structured actions, but it is still embedded inside the older node/intent architecture. The result is a hybrid system:

- subagent extraction
- planner output
- intent queueing
- node routing
- fallback natural-language streaming

This is functional, but it is more complex than needed for the target stack.

## 3. Current data access shape

### 3.1 Raw SQL helper layer

The repo’s `tools.py` file contains many data operations as string-built SQL helpers / LangChain tools. The current file mixes:

- place lookup
- postcode lookup
- theme lookup
- cube lookup
- unit lookup
- data entity lookup
- place information
- unit type information
- cube data retrieval

These are useful business capabilities, but they currently live in a low-level form that is hard to test and too easy to extend unsafely.

### 3.2 Query generation risks

A lot of queries are interpolated directly into SQL strings. Even though this is a prototype and some inputs are constrained, the rewrite should treat this as technical debt and replace it with parameterized repository methods.

### 3.3 Map delivery via GeoPandas cache

Polygon delivery currently relies on a custom `PolygonCache` that:

- opens DB connections directly
- fetches WKB geometries
- converts them with GeoPandas/Shapely
- caches by unit type / year / feature ids
- serves GeoJSON through Flask routes

This works for a prototype, but it couples map serving, caching, spatial querying, and serialization into one local utility.

## 4. Current frontend state and visualization shape

### 4.1 Many Dash stores as the state bus

The frontend keeps a substantial amount of shared state in `dcc.Store` objects. This includes:

- map state
- place state
- app state
- thread id
- SSE state
- map interaction flags
- visualization control state

This is workable, but the data contracts are mostly implicit.

### 4.2 Visualization callbacks fetch DB data directly through tool helpers

The visualization callback is doing more than UI rendering:

- chooses cube options
- fetches cube data
- constructs line charts
- constructs category pies
- constructs table output
- applies theme filtering logic

In the target architecture, this logic should be split between:

- backend data service / render-spec service
- frontend rendering callback

## 5. Current auth and deployment shape

### 5.1 Auth is embedded in the Dash server

The current prototype uses Flask-Login and a SQLite-backed users table. This is worth preserving functionally because it already solves a real need: the app is not wide open.

### 5.2 Container shape is single-process-plus-Redis

The Dockerfile starts Redis in the same container and then runs Gunicorn for the Dash app. Docker Compose exposes one service and expects PostgreSQL and Ollama to exist externally.

This means the current stack is:

- one main app container
- internal Redis side process
- external Postgres
- external Ollama

For the rewrite, Redis should disappear unless there is a strong demonstrated need.

## 6. Architectural pain points to solve

### 6.1 Too many responsibilities in one process

Today, one runtime is responsible for:

- auth
- UI rendering
- workflow orchestration
- model streaming
- map APIs
- data querying
- visualization assembly
- state persistence

That makes it harder to reason about failures and harder to test individual pieces.

### 6.2 Business logic is spread across too many patterns

The same user intent can currently touch several layers:

- Dash callbacks
- SSE adapter
- LangGraph nodes
- planner logic
- tools.py SQL helpers
- state-schema helpers

The rewrite should collapse that into a cleaner chain:

`frontend intent -> typed API request -> orchestrator -> deterministic service/repository methods -> typed response/render spec`

### 6.3 Missing semantic boundary between LLM and schema

The app already contains important domain operations, but there is still not a first-class semantic layer that explicitly models:

- place resolution
- reporting unit resolution
- theme resolution
- time-series requests
- map layer requests
- provenance / metadata requests

That semantic layer is the key missing piece.

## 7. Functional inventory to preserve

Codex should assume all of the following must continue to work after the rewrite:

- login / logout
- thread creation
- chat send + streamed response
- place search and disambiguation
- postcode lookup
- map polygon loading by unit type and by ids
- selected places reflected in map and chart state
- theme listing / selection / description
- fetching cubes for a selected place/theme
- line chart visualization over time
- category breakdown visualizations where the data supports it
- data table tab
- place information lookups
- unit type information lookups
- data entity information lookups

## 8. Delete/replace candidates in the rewrite

Once parity is achieved, plan to remove or replace most of the following current modules:

- `src/vobchat/app.py` as the all-in-one composition root
- `src/vobchat/workflow.py`
- `src/vobchat/workflow_sse_adapter.py`
- `src/vobchat/conversational_agent.py`
- `src/vobchat/intent_handling.py`
- `src/vobchat/intent_subagents.py`
- `src/vobchat/nodes/` (entire pattern unless a specific helper survives in a simpler home)
- `src/vobchat/tools.py` in its current form
- `src/vobchat/utils/polygon_cache.py`
- Redis checkpoint / Redis pool utilities tied only to the old workflow architecture
- callback code that mixes transport, orchestration, and rendering concerns

## 9. Rewrite stance

This repo is still in prototype stage, so the rewrite should be opinionated:

- preserve **behavior**, not architecture
- replace broad dynamic state dictionaries with typed schemas where possible
- move all data access behind repositories/services
- remove superseded code instead of keeping it around for comfort
