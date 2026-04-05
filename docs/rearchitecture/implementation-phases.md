# Implementation phases

This is the ordered plan Codex should follow on the rewrite branch.

## Ground rules

- Do not try to preserve old architecture.
- Do not leave old code behind after a replacement path is verified.
- Prefer moving to typed contracts early.
- Keep the app usable as often as possible, but do not contort the design just to keep every intermediate commit runnable.
- When a phase is complete, update `parity-and-progress.md` in the same branch.

---

## Phase 0 — branch setup and planning docs

### Tasks

- create the rewrite branch
- add the docs in `docs/rearchitecture/`
- confirm target repo shape and naming

### Exit criteria

- planning docs exist and are internally consistent

---

## Phase 1 — package reshape and dependency cleanup

### Tasks

- introduce the new package layout under:
  - `src/vobchat/web/`
  - `src/vobchat/api/`
  - `src/vobchat/core/`
  - `src/vobchat/db/`
  - `src/vobchat/auth/`
- move reusable auth code into `src/vobchat/auth/`
- introduce typed settings and shared logging setup
- replace ad-hoc imports of env vars with centralized settings
- split requirements into a cleaner dependency set
- remove dependencies that only exist for the old architecture, once no longer used

### Guidance

Start by creating the new folders and entrypoints without deleting the old code immediately. This gives Codex room to build the new path cleanly. But once a new entrypoint is live and covered, delete the superseded old one.

### Exit criteria

- new package layout exists
- web and api entrypoints can both start
- settings are centralized

---

## Phase 2 — semantic DB layer

### Tasks

- create a real DB engine/session module using SQLAlchemy 2 + psycopg3
- implement repository modules for:
  - places
  - themes
  - series/cubes
  - maps
  - metadata
- move reusable SQL into parameterized query helpers or SQL files
- add read-only DB session enforcement where possible
- define typed return models at the repository/service boundary

### Minimum repository methods

- `search_places_exact`
- `search_places_fuzzy`
- `lookup_postcode_units`
- `list_themes`
- `lookup_theme`
- `list_cubes_for_unit_theme`
- `fetch_series_for_units_and_cubes`
- `fetch_category_breakdown`
- `fetch_place_profile`
- `fetch_unit_type_info`
- `fetch_data_entity_info`
- `fetch_map_features_by_bbox`
- `fetch_map_features_by_ids`

### Guidance

This phase is not just “move SQL into new files”. It is where Codex should enforce the semantic boundary that the planner and frontend will rely on.

### Exit criteria

- no new code in the rewrite path uses `tools.py`
- repository methods cover all currently required app behaviors
- queries are parameterized

---

## Phase 3 — API contracts and FastAPI routers

### Tasks

- define Pydantic models for all request/response contracts
- implement FastAPI routers for:
  - `/chat`
  - `/places`
  - `/themes`
  - `/maps`
  - `/series`
  - `/metadata`
- add health/readiness endpoints
- add provenance fields to responses where applicable

### Suggested endpoints

- `POST /chat/turn`
- `GET /chat/stream/{thread_id}`
- `GET /places/search`
- `GET /places/postcode`
- `GET /places/{place_id}`
- `GET /themes`
- `GET /themes/resolve`
- `POST /series/time`
- `POST /series/categories`
- `GET /maps/features`
- `GET /maps/features/by-ids`
- `GET /metadata/unit-types/{unit_type}`
- `GET /metadata/entities/{entity_id}`

### Exit criteria

- frontend-relevant endpoints exist with stable schemas
- API can serve place, theme, map, and series requests without any Dash dependency

---

## Phase 4 — chat orchestrator and local-model integration

### Tasks

- build an OpenAI-compatible LLM client wrapper in `core/llm/client.py`
- implement a planner that produces typed actions instead of arbitrary dict blobs
- implement a chat orchestrator service that:
  1. reads conversation state
  2. resolves intent and missing parameters
  3. calls deterministic service methods
  4. builds assistant text + UI deltas/render specs
- implement SSE streaming from FastAPI

### Important constraints

- do not rebuild the old LangGraph node graph unless a requirement clearly demands it
- do not let the model produce raw SQL
- do not emit raw Plotly code from the model; emit chart intent / render spec only

### Exit criteria

- one end-to-end chat turn works through FastAPI
- streamed text reaches a client
- planner can drive at least place selection, theme selection, and chart request flows

---

## Phase 5 — web frontend rewrite onto API contracts

### Tasks

- create a new Dash app composition root under `src/vobchat/web/app.py`
- keep the same core UX zones:
  - chat pane
  - map pane
  - visualization pane
  - table/provenance pane
- replace direct DB / workflow coupling with API client calls
- reduce store sprawl to a smaller typed state model
- render charts from backend render specs or backend-ready datasets
- wire map interactions to API-backed place/unit resolution

### Guidance

The frontend rewrite should be selective, not sentimental. Reuse UI ideas, not transport logic.

### Exit criteria

- new Dash app can authenticate, start a chat, load polygons, and render charts through the API layer

---

## Phase 6 — auth-preserving proxy path

### Tasks

- keep existing login/logout behavior available from the web app
- move auth code into `src/vobchat/auth/`
- add authenticated proxy routes on the web side for API calls / streams
- keep FastAPI private to the internal network during this phase

### Guidance

This is the simplest way to preserve current functionality without solving cross-origin browser auth during the same rewrite.

### Exit criteria

- user logs in once
- authenticated browser session can use chat/map/chart features through the new path

---

## Phase 7 — parity completion

### Tasks

Confirm parity for:

- exact and fuzzy place search
- postcode-based selection
- place disambiguation
- map polygon loading by unit type, bbox, and ids
- theme listing and selection
- time-series plotting
- category visualization
- data table rendering
- place profile information
- unit type info
- data entity info
- thread minting / chat history continuity

### Exit criteria

- every item in `parity-and-progress.md` is either complete or explicitly descoped with justification

---

## Phase 8 — hard cleanup

### Tasks

Delete the old implementation pieces that are superseded, including as appropriate:

- old all-in-one app composition root
- LangGraph workflow and node graph modules
- Redis checkpoint/pool utilities
- old callback paths that query through `tools.py`
- `tools.py` in its current prototype form
- polygon cache utilities if the new map layer replaces them
- stale requirements and Docker references

### Guidance

Do not stop at “new code added”. The branch is only done when the obsolete code is gone.

### Exit criteria

- no duplicate architecture remains
- dependency list reflects the new system, not the old one

---

## Phase 9 — tests, docs, and deployment polish

### Tasks

- add unit tests for repositories and services
- add API tests for routers and schema contracts
- add integration tests for core user journeys
- update README and deployment docs
- update Dockerfile / docker-compose to the new two-service shape

### Minimum test journeys

- login -> select place -> select theme -> render line chart
- login -> search postcode -> resolve place -> load map polygons
- login -> ask for place info / unit type info / data entity info
- login -> category-capable chart request -> category visualization

### Exit criteria

- docs reflect the new architecture
- deployment instructions work
- tests cover the critical parity paths

---

## Recommended order of actual coding

If Codex needs a finer-grained sequence, use this order:

1. new package layout + settings
2. DB engine + repositories
3. Pydantic schemas
4. FastAPI routers
5. chat orchestrator + local LLM client
6. new Dash app shell
7. same-origin proxy/auth integration
8. map/chart/table parity
9. deletion of old code
10. tests/docs cleanup
