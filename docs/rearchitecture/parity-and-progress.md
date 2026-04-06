# Parity and progress tracker

This file is the operational checklist for the rewrite. Codex should update it as work lands.

## Status legend

- `[ ]` not started
- `[-]` in progress
- `[x]` complete
- `[!]` blocked / needs decision

---

## 1. User-visible parity checklist

### Authentication

- [x] login page works
- [x] login with existing user works
- [x] logout works
- [x] unauthorized users are redirected appropriately

### Chat

- [x] new thread can be created
- [x] user can send a free-text message
- [x] assistant text streams back incrementally
- [x] chat history persists within a session/thread
- [x] clarifying questions are supported

### Place selection

- [x] exact place search works
- [x] fuzzy place search works
- [x] ambiguous places can be disambiguated
- [x] multiple selected places are supported
- [x] place removal works
- [x] postcode lookup works

### Themes / metadata

- [x] theme list can be shown
- [x] theme resolution from chat works
- [x] theme description works
- [x] place profile information works
- [x] unit type information works
- [x] data entity information works

### Map

- [x] polygons can load by unit type
- [x] polygons can load by bbox
- [x] polygons can load by ids / current selection
- [x] year filters affect map results correctly
- [x] map selection updates chat/app state
- [x] selected places are visibly reflected on the map

### Charts / tables

- [x] cube/measure options are available for the selected place/theme
- [x] time-series line chart renders
- [x] multiple places can be compared on one chart
- [x] category visualization renders when applicable
- [x] data table tab renders underlying rows
- [x] chart updates stay in sync with theme/place changes

### Provenance / transparency

- [ ] returned data includes enough metadata for the UI to explain what is shown
- [ ] source/provenance bundle is available for plotted data

---

## 2. Backend/API completion checklist

### Repository layer

- [x] places repository implemented
- [x] themes repository implemented
- [x] series repository implemented
- [x] maps repository implemented
- [x] metadata repository implemented
- [x] all queries parameterized
- [x] read-only DB usage enforced

### API layer

- [x] `/chat` endpoints implemented
- [x] `/places` endpoints implemented
- [x] `/themes` endpoints implemented
- [x] `/maps` endpoints implemented
- [x] `/series` endpoints implemented
- [x] `/metadata` endpoints implemented
- [x] health/readiness endpoints implemented

### LLM/orchestrator layer

- [x] OpenAI-compatible local-model client implemented
- [x] typed planner implemented
- [x] deterministic tool/service invocation implemented
- [x] SSE streaming implemented
- [x] no arbitrary SQL generation path remains

### Web layer

- [x] new Dash app composition root exists
- [x] same-origin proxy routes exist
- [x] frontend API client exists
- [x] frontend stores simplified
- [x] old direct DB-calling callbacks removed

---

## 3. Cleanup checklist

These items should not remain after the rewrite is complete unless explicitly justified.

- [x] old monolithic `src/vobchat/app.py` path removed or reduced to a thin wrapper that immediately delegates to the new web entrypoint
- [x] `workflow.py` removed
- [x] `workflow_sse_adapter.py` removed
- [x] `conversational_agent.py` removed from the runtime path
- [x] old intent/node graph removed
- [x] `tools.py` removed or reduced to non-runtime compatibility helpers that are genuinely still needed
- [x] Redis checkpoint/pool utilities removed
- [x] obsolete polygon cache removed
- [x] stale requirements removed
- [x] stale Docker/Compose config removed

---

## 4. Testing checklist

### Unit tests

- [x] repository tests
- [ ] service tests
- [x] planner/orchestrator tests
- [ ] render-spec tests
- [x] Phase 1 smoke tests

### API tests

- [x] places router tests
- [x] themes router tests
- [x] maps router tests
- [x] series router tests
- [x] metadata router tests
- [x] chat router tests

### Integration tests

- [x] login -> place -> theme -> line chart
- [x] login -> postcode -> map selection
- [x] login -> place info / unit type info / entity info
- [x] login -> category view when supported

---

## 5. Decisions log

Use this section for architecture decisions made during implementation.

### Decision template

```md
## YYYY-MM-DD — Decision title
- Context:
- Decision:
- Consequences:
```

## 2026-04-06 — Phase 5 web runtime uses non-streaming chat turns
- Context:
  - The FastAPI chat backend already supports SSE, but browser-auth-preserving SSE proxying is explicitly a Phase 6 concern.
- Decision:
  - The Phase 5 Dash runtime uses server-side API client calls plus non-streaming `POST /chat/turn`.
- Consequences:
  - The rewrite runtime now uses the new chat/domain API contracts without dragging same-origin SSE/auth complexity into this phase.
  - Incremental assistant streaming remains available in the backend and is ready to be integrated in Phase 6.

## 2026-04-06 — Phase 6 replaces the Phase 5 chat fallback with same-origin proxy streaming
- Context:
  - The Dash runtime had already moved onto the new backend contracts, but the active chat path still used server-side non-streaming callback calls.
- Decision:
  - The browser chat runtime now talks only to same-origin authenticated web proxy routes for thread creation, turn submission, and SSE event streaming.
  - Read-only domain requests remain server-side in Dash callbacks for now; Phase 6 uses the minimal chat-focused proxy pattern.
- Consequences:
  - The browser no longer needs direct FastAPI access for the active chat path.
  - Existing session-cookie auth remains the only browser auth mechanism.
  - A thin non-streaming helper remains in the repo for tests and fallback logic, but it is no longer the primary runtime path.

## 2026-04-06 — Keep `src/vobchat/app.py` as the only legacy-facing shim
- Context:
  - The rewrite runtime now lives under `src/vobchat/web/`, but existing commands and some operator expectations still point at `vobchat.app:server`.
- Decision:
  - Keep `src/vobchat/app.py` as a tiny delegating wrapper to `vobchat.web.app` and remove the rest of the legacy top-level runtime shims.
- Consequences:
  - The active architecture is unambiguous in the source tree.
  - Existing entrypoint expectations continue to work without carrying a second runtime implementation.

---

## 6. Progress log

Use this section as a running work log.

### Update template

```md
## YYYY-MM-DD
- Completed:
- In progress:
- Next:
- Risks/blockers:
```

### Updates

## 2026-04-05
- Completed:
  - Investigated the current repo architecture.
  - Added rewrite planning docs under `docs/rearchitecture/`.
  - Defined the target split between Dash web layer and FastAPI API layer.
- In progress:
  - None yet; implementation has not started.
- Next:
  - Reshape package layout and create new entrypoints.
  - Build the typed DB/repository layer.
- Risks/blockers:
  - Auth handoff between web and API must stay simple.
  - Need to ensure feature parity before deleting the old prototype path.

## 2026-04-05
- Completed:
  - Created the Phase 1 package scaffold under `web/`, `api/`, `core/`, `db/`, and `auth/`.
  - Moved the real Dash composition root to `src/vobchat/web/app.py` and reduced `src/vobchat/app.py` to a delegating shim.
  - Added centralized typed settings, centralized logging, a minimal FastAPI app, and relocated auth models/routes/CLI under `src/vobchat/auth/`.
  - Added lightweight Phase 1 smoke tests and cleaned dependency metadata for the new web/api split.
- In progress:
  - Same-origin proxy work is only scaffolded in Phase 1; real proxy endpoints are still pending.
- Next:
  - Build the SQLAlchemy-based repository layer and start replacing direct `tools.py`/prototype DB access.
  - Expand FastAPI beyond health endpoints into typed domain routers.
- Risks/blockers:
  - The legacy Dash path still depends on the old workflow/Redis stack until later phases replace it.
  - The Phase 1 smoke path intentionally skips workflow startup to keep structural tests independent from Redis.

## 2026-04-05
- Completed:
  - Added the real SQLAlchemy + psycopg3 DB foundation under `src/vobchat/db/engine.py` and `src/vobchat/db/session.py`.
  - Implemented typed repository models plus repository modules for places, themes, series, maps, and metadata.
  - Moved core query behavior off the old helper style into parameterized repository methods with read-only execution semantics.
  - Added semantic SQL foundation docs/assets and focused repository tests.
- In progress:
  - The old runtime still uses legacy helpers in places; Phase 2 establishes the new path but does not rewire the full app yet.
- Next:
  - Build Phase 3 API contracts and routers on top of the repository layer.
  - Start moving the rewrite-path consumers away from legacy helper modules and toward the new repositories/services.
- Risks/blockers:
  - The current prototype runtime still has legacy `tools.py`/`config.py` dependencies outside the rewrite path.
  - Repository queries are validated by interface/normalization tests in this phase; broader live-DB integration coverage is still desirable later.

## 2026-04-05
- Completed:
  - Added typed Pydantic API contracts for places, themes, series, maps, and metadata under `src/vobchat/api/schemas/`.
  - Implemented real FastAPI routers for `/places`, `/themes`, `/series`, `/maps`, and `/metadata`, all backed by the Phase 2 repository/services path rather than `tools.py`.
  - Added thin API services to translate repository outputs into stable response models and keep router code small.
  - Added focused FastAPI tests with dependency overrides for the new domain routers and preserved the health/readiness checks.
- In progress:
  - `/chat` remains a placeholder until the orchestrator work in Phase 4.
- Next:
  - Build the typed chat/orchestrator layer and add `/chat` endpoints.
  - Start moving the web rewrite path onto these API contracts instead of legacy direct-data callbacks.
- Risks/blockers:
  - The Dash frontend still uses legacy runtime paths, so user-visible parity is not complete even though the API surface now exists.
  - Provenance fields are still minimal and should expand alongside render-spec work in later phases.

## 2026-04-06
- Completed:
  - Added typed chat/thread contracts under `src/vobchat/api/schemas/chat.py`, including typed planner results and SSE payloads.
  - Implemented a simple in-memory rewrite-path thread store with per-thread SSE event fanout.
  - Added an OpenAI-compatible local-model client, typed planner prompts/schema helpers, and a planner with LLM-first behavior plus safe fallback heuristics.
  - Implemented the deterministic chat orchestrator and wired `/chat/threads`, `/chat/turn`, and `/chat/stream/{thread_id}` into the FastAPI app.
  - Added planner, orchestrator, and chat router tests for the Phase 4 path.
- In progress:
  - The web layer still needs to move onto the new chat endpoints and SSE path in later phases.
- Next:
  - Start the Phase 5 frontend rewrite onto the new typed chat/domain API contracts.
  - Expand render-spec/provenance output so the frontend can render richer structured results without parsing prose.
- Risks/blockers:
  - Thread state is intentionally in-memory only in Phase 4, so it is not durable across process restarts.
  - The rewrite path now avoids model-driven SQL, but the legacy runtime still exists elsewhere in the repo until later cleanup phases remove it.

## 2026-04-06
- Completed:
  - Rewrote the real Dash runtime in `src/vobchat/web/app.py` so it composes the Phase 5 `web/` stores, components, and callbacks instead of the legacy workflow/SSE/polygon stack.
  - Expanded `src/vobchat/web/clients/api_client.py` into a typed transport client for chat, places, themes, series, maps, and metadata endpoints.
  - Replaced the legacy rewrite-path callbacks with server-side callbacks that create threads, submit chat turns, apply structured deltas, load maps via `/maps/*`, load charts/tables via `/series/*`, and hydrate metadata via `/metadata/*`.
  - Simplified the rewrite-path Dash stores into thread, selection, map, visualization, metadata, and request-status stores.
  - Added focused Phase 5 tests for the web API client, chat turn state application, map loading, and visualization loading.
- In progress:
  - Browser-side streaming remains deferred to Phase 6.
  - BBox-driven map loading is still pending; Phase 5 loads by unit type and selected ids.
- Next:
  - Add the auth-preserving same-origin proxy layer for browser-facing API/SSE traffic.
  - Complete the remaining user-visible parity items such as browser streaming and map-to-chat selection.
- Risks/blockers:
  - Loading all features for a selected unit type is a workable Phase 5 baseline, but dense unit types will benefit from the later bbox/proxy work.

## 2026-04-06
- Completed:
  - Replaced the Phase 5 proxy placeholder with real authenticated same-origin chat proxy routes under `src/vobchat/web/proxy/routes.py`.
  - Added browser-facing same-origin proxy endpoints for thread creation, thread lookup, turn submission, and SSE stream relay while keeping the FastAPI backend internal.
  - Switched the active Dash chat runtime onto a browser-side SSE client that creates threads through the proxy, submits `stream=true` turns through the proxy, applies structured `ui.delta` updates, and finalizes from `turn.completed`.
  - Added focused Phase 6 tests for proxy auth/ownership, proxied turn submission, SSE relay behavior, layout/runtime config, and streamed state helper behavior.
- In progress:
  - Non-chat domain proxying remains intentionally minimal in this phase; maps, series, and metadata still use server-side Dash callbacks.
- Next:
  - Finish the remaining parity and cleanup work in Phase 7, including broader browser/UI parity items and deletion of superseded legacy runtime pieces.
- Risks/blockers:
  - The proxy thread ownership registry is intentionally in-memory in this phase, matching the in-memory chat thread store.
  - The active browser chat path now streams through the web layer, but full same-origin proxy coverage for every domain endpoint is deliberately deferred.

## 2026-04-06
- Completed:
  - Re-audited the real rewrite runtime against the parity checklist instead of assuming earlier checkmarks were accurate.
  - Tightened planner/orchestrator behavior for fuzzy search fallback, ambiguous place disambiguation via explicit place-id selection, place removal by name, place-aware theme listing, place-aware map requests, and data-entity resolution that also hydrates entity info.
  - Added session-backed rewrite-path stores plus thread snapshot hydration so place/theme/cube/map/chart state can be restored from `GET /chat/threads/{thread_id}` after reload.
  - Completed rewrite-path parity for bbox-driven map loading, map click -> place selection, multi-place chart comparison, category/table rendering, theme descriptions in the UI, and metadata hydration for place profile, key findings, unit type info, and data entity info.
  - Added Phase 7 parity-oriented tests covering auth redirects/login/logout, chat journeys for exact/fuzzy/ambiguous/postcode/remove/theme/chart/category/metadata flows, thread reload continuity, map bbox + click selection, and multi-place visualization/table metadata hydration.
- In progress:
  - Provenance/transparency remains intentionally incomplete; the rewrite runtime returns useful typed datasets, but it still does not expose a full provenance bundle for plotted data.
- Next:
  - Start Phase 8 hard cleanup and remove superseded legacy runtime pieces now that rewrite-path parity is functionally complete.
- Risks/blockers:
  - Chat thread persistence is still in-memory only, so continuity survives browser reloads within the active server process but not backend restarts.
  - The testing checklist still lacks full browser-level end-to-end coverage even though the parity journeys are now exercised across API, proxy, and web helper layers.

## 2026-04-06
- Completed:
  - Deleted the superseded LangGraph/Redis runtime, including the legacy workflow modules, intent graph, prototype tools/helpers, old polygon routes/cache path, and the old top-level `callbacks/` and `components/` wrapper packages.
  - Removed obsolete compatibility modules (`cli.py`, `models.py`, `config.py`, `configure_logging.py`, `llm_factory.py`) and old prototype scripts/tests that only existed for the deleted architecture.
  - Pruned the dependency set to the active Dash/FastAPI/SQLAlchemy/httpx stack and removed old workflow, Redis, GeoPandas/Shapely, psycopg2, and SSH-tunnel requirements.
  - Reworked Docker and Compose to reflect the active split architecture: public web service, internal API service, no Redis, and `vobchat.web.app` as the web entrypoint.
  - Added a cleanup-focused smoke test proving the active entrypoints still import while removed legacy modules no longer exist.
- In progress:
  - None for Phase 8. The remaining provenance bundle gap is unchanged and intentionally deferred.
- Next:
  - Phase 9 docs and deployment polish.
  - Decide whether any additional operator-facing docs should move from the legacy `vobchat.app` wording to the explicit `vobchat.web.app` / `vobchat.api.main` split.
- Risks/blockers:
  - `src/vobchat/app.py` remains intentionally as the only tiny wrapper for entrypoint compatibility.
  - Provenance/transparency is still the only meaningful feature gap left in the tracker; cleanup did not widen it.

## 2026-04-06
- Completed:
  - Rewrote the root `README.md` so it reflects the active Dash web app, internal FastAPI API, same-origin chat proxy, typed repository stack, and current run/deploy/test commands.
  - Added `docs/runtime-architecture.md` as a concise maintainer-facing runtime boundary and request-flow summary.
  - Simplified packaging so `pyproject.toml` is the canonical package metadata, added a small `dev` extra for pytest, removed the redundant `setup.py`, and documented `requirements.txt` as the runtime dependency mirror for plain installs and container builds.
  - Clarified `.env.example`, local development guidance, Compose service roles, entrypoints, auth CLI usage, and the retained `src/vobchat/app.py` compatibility shim.
  - Ran the final validation pass, including compile checks, packaging install validation, Flask auth CLI validation, Compose config validation, and the targeted regression suite.
- In progress:
  - None. The rewrite is ready for review/merge.
- Next:
  - Review and merge the rewrite branch.
  - Decide when to prioritize the remaining provenance/transparency work.
- Risks/blockers:
  - Provenance/transparency is still incomplete and remains the only meaningful product gap called out in this tracker.
  - Chat thread state is still intentionally in-memory and is not durable across backend restarts.
