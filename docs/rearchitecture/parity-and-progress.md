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

- [ ] login page works
- [ ] login with existing user works
- [ ] logout works
- [ ] unauthorized users are redirected appropriately

### Chat

- [ ] new thread can be created
- [ ] user can send a free-text message
- [ ] assistant text streams back incrementally
- [ ] chat history persists within a session/thread
- [ ] clarifying questions are supported

### Place selection

- [ ] exact place search works
- [ ] fuzzy place search works
- [ ] ambiguous places can be disambiguated
- [ ] multiple selected places are supported
- [ ] place removal works
- [ ] postcode lookup works

### Themes / metadata

- [ ] theme list can be shown
- [ ] theme resolution from chat works
- [ ] theme description works
- [ ] place profile information works
- [ ] unit type information works
- [ ] data entity information works

### Map

- [ ] polygons can load by unit type
- [ ] polygons can load by bbox
- [ ] polygons can load by ids / current selection
- [ ] year filters affect map results correctly
- [ ] map selection updates chat/app state
- [ ] selected places are visibly reflected on the map

### Charts / tables

- [ ] cube/measure options are available for the selected place/theme
- [ ] time-series line chart renders
- [ ] multiple places can be compared on one chart
- [ ] category visualization renders when applicable
- [ ] data table tab renders underlying rows
- [ ] chart updates stay in sync with theme/place changes

### Provenance / transparency

- [ ] returned data includes enough metadata for the UI to explain what is shown
- [ ] source/provenance bundle is available for plotted data

---

## 2. Backend/API completion checklist

### Repository layer

- [ ] places repository implemented
- [ ] themes repository implemented
- [ ] series repository implemented
- [ ] maps repository implemented
- [ ] metadata repository implemented
- [ ] all queries parameterized
- [ ] read-only DB usage enforced

### API layer

- [ ] `/chat` endpoints implemented
- [ ] `/places` endpoints implemented
- [ ] `/themes` endpoints implemented
- [ ] `/maps` endpoints implemented
- [ ] `/series` endpoints implemented
- [ ] `/metadata` endpoints implemented
- [ ] health/readiness endpoints implemented

### LLM/orchestrator layer

- [ ] OpenAI-compatible local-model client implemented
- [ ] typed planner implemented
- [ ] deterministic tool/service invocation implemented
- [ ] SSE streaming implemented
- [ ] no arbitrary SQL generation path remains

### Web layer

- [ ] new Dash app composition root exists
- [ ] same-origin proxy routes exist
- [ ] frontend API client exists
- [ ] frontend stores simplified
- [ ] old direct DB-calling callbacks removed

---

## 3. Cleanup checklist

These items should not remain after the rewrite is complete unless explicitly justified.

- [ ] old monolithic `src/vobchat/app.py` path removed or reduced to a thin wrapper that immediately delegates to the new web entrypoint
- [ ] `workflow.py` removed
- [ ] `workflow_sse_adapter.py` removed
- [ ] `conversational_agent.py` removed from the runtime path
- [ ] old intent/node graph removed
- [ ] `tools.py` removed or reduced to non-runtime compatibility helpers that are genuinely still needed
- [ ] Redis checkpoint/pool utilities removed
- [ ] obsolete polygon cache removed
- [ ] stale requirements removed
- [ ] stale Docker/Compose config removed

---

## 4. Testing checklist

### Unit tests

- [ ] repository tests
- [ ] service tests
- [ ] planner/orchestrator tests
- [ ] render-spec tests

### API tests

- [ ] places router tests
- [ ] themes router tests
- [ ] maps router tests
- [ ] series router tests
- [ ] metadata router tests
- [ ] chat router tests

### Integration tests

- [ ] login -> place -> theme -> line chart
- [ ] login -> postcode -> map selection
- [ ] login -> place info / unit type info / entity info
- [ ] login -> category view when supported

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
